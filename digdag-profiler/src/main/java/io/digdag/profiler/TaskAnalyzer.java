package io.digdag.profiler;

import com.google.inject.Injector;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.digdag.client.DigdagClient.objectMapper;

public class TaskAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(TaskAnalyzer.class);

    private final Injector injector;

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

    public void run(PrintStream printStream,
                    Instant createdFrom,
                    Instant createdTo,
                    int fetchedAttempts,
                    int partitionSize,
                    int databaseWaitMillis)
            throws IOException
    {
        TransactionManager tm = injector.getInstance(TransactionManager.class);
        ProjectStoreManager pm = injector.getInstance(ProjectStoreManager.class);
        SessionStoreManager sm = injector.getInstance(SessionStoreManager.class);

        AtomicLong lastId = new AtomicLong();
        TasksSummary.Builder tasksSummaryBuilder = new TasksSummary.Builder();
        logger.info("Task analysis started");

        while (true) {
            logger.debug("Collecting attempts");

            // Paginate to avoid too long transaction
            AtomicLong counter = new AtomicLong();
            Map<Long, List<StoredSessionAttemptWithSession>> partitionedAttemptIds = tm.begin(() -> {
                List<StoredSessionAttemptWithSession> attempts =
                        sm.findFinishedAttemptsWithSessions(createdFrom, createdTo, lastId.get(), fetchedAttempts);
                attempts.stream().mapToLong(StoredSessionAttemptWithSession::getId).max().ifPresent((id) ->
                        lastId.set(Math.max(lastId.get(), id))
                );
                // Divide many StoredSessionAttemptWithSession instances into some partitions for the following procedures
                // which may hold resources for a while
                return attempts.stream()
                        .collect(Collectors.groupingBy((attempt) -> counter.getAndIncrement() / partitionSize));
            });
            logger.debug("Collected {} attempts", counter.get());
            waitAfterDatabaseAccess(databaseWaitMillis);

            if (partitionedAttemptIds.isEmpty()) {
                break;
            }

            // Process partitioned attemptIds list from the beginning
            for (long attemptIdsGroup : partitionedAttemptIds.keySet().stream().sorted().collect(Collectors.toList())) {
                List<StoredSessionAttemptWithSession> attemptsWithSessions = partitionedAttemptIds.get(attemptIdsGroup);
                logger.debug("Processing {} attempts", attemptsWithSessions.size());
                tm.begin(() -> {
                    for (StoredSessionAttemptWithSession attemptWithSession : attemptsWithSessions) {
                        // TODO: Reduce these round-trips with database to improve the performance
                        int projectId = attemptWithSession.getSession().getProjectId();
                        int siteId;
                        try {
                            siteId = pm.getProjectByIdInternal(projectId).getSiteId();
                        } catch (ResourceNotFoundException e) {
                            logger.error(String.format("Can't find the project: %d. Just skipping it...", projectId), e);
                            continue;
                        }
                        List<ArchivedTask> tasks = sm.getSessionStore(siteId).getTasksOfAttempt(attemptWithSession.getId());
                        TasksSummary.updateBuilderWithTasks(tasksSummaryBuilder, tasks);
                    }
                    logger.debug("Processed {} attempts", attemptsWithSessions.size());
                    return null;
                });
                waitAfterDatabaseAccess(databaseWaitMillis);
            }
        }

        logger.info("Task analysis finished");

        objectMapper().writeValue(printStream, tasksSummaryBuilder.build());
    }
}
