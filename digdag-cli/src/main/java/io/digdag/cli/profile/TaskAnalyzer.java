package io.digdag.cli.profile;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.SessionStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TaskAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(TaskAnalyzer.class);

    private final Injector injector;

    private static class AttemptIdWithSiteId
    {
        public final int siteId;
        public final long attemptId;

        public AttemptIdWithSiteId(int siteId, long attemptId)
        {
            this.siteId = siteId;
            this.attemptId = attemptId;
        }
    }

    public TaskAnalyzer(Injector injector)
    {
        this.injector = injector;
    }

    private void waitAfterDatabaseAccess(int waitMillis)
    {
        try {
            TimeUnit.MILLISECONDS.sleep(waitMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public WholeTasksSummary run(
            Instant finishedFrom,
            Instant finishedTo,
            int fetchedAttempts,
            int partitionSize,
            int databaseWaitMillis)
    {
        TransactionManager tm = injector.getInstance(TransactionManager.class);
        SessionStoreManager sm = injector.getInstance(SessionStoreManager.class);

        AtomicLong lastId = new AtomicLong();
        WholeTasksSummary.Builder tasksSummaryBuilder = new WholeTasksSummary.Builder();
        logger.debug("Task analysis started");

        while (true) {
            logger.debug("Collecting attempts");

            // Partition target attempts to avoid a too long transaction later
            List<List<AttemptIdWithSiteId>> attemptIdsList = tm.begin(() ->
                    Lists.partition(
                            sm.findFinishedAttemptsWithSessions(finishedFrom, finishedTo, lastId.get(), fetchedAttempts).stream()
                                    .map(attempt -> {
                                        lastId.set(Math.max(lastId.get(), attempt.getId()));
                                        return new AttemptIdWithSiteId(attempt.getSiteId(), attempt.getId());
                                    })
                                    .collect(Collectors.toList()),
                            partitionSize));

            logger.debug("Collected {} attempts", attemptIdsList.stream().mapToInt(List::size).sum());
            waitAfterDatabaseAccess(databaseWaitMillis);

            if (attemptIdsList.isEmpty()) {
                break;
            }

            // Process partitioned attemptIds list from the beginning
            for (List<AttemptIdWithSiteId> attemptIds : attemptIdsList) {
                logger.debug("Processing {} attempts", attemptIds.size());
                tm.begin(() -> {
                    for (AttemptIdWithSiteId attemptIdWithSiteId : attemptIds) {
                        List<ArchivedTask> tasks = sm.getSessionStore(attemptIdWithSiteId.siteId).getTasksOfAttempt(attemptIdWithSiteId.attemptId);
                        tasksSummaryBuilder.updateWithTasks(attemptIdWithSiteId.siteId, tasks);
                    }
                    logger.debug("Processed {} attempts", attemptIds.size());
                    return null;
                });
                waitAfterDatabaseAccess(databaseWaitMillis);
            }
        }

        logger.debug("Task analysis finished");

        return tasksSummaryBuilder.build();
    }
}
