package io.digdag.cli;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import javax.annotation.PreDestroy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.workflow.Tasks;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskReport;
import io.digdag.client.config.ConfigFactory;

class ResumeStateManager
{
    private static Logger logger = LoggerFactory.getLogger(ResumeStateManager.class);

    private final ConfigFactory cf;
    private final SessionStoreManager sessionStoreManager;
    private final ObjectMapper mapper;
    private final List<ResumeStateDir> managedDirs;

    private ScheduledExecutorService executor = null;

    @Inject
    private ResumeStateManager(ConfigFactory cf, SessionStoreManager sessionStoreManager, ObjectMapper mapper)
    {
        this.cf = cf;
        this.sessionStoreManager = sessionStoreManager;
        this.mapper = mapper;
        this.managedDirs = new CopyOnWriteArrayList<>();
    }

    TaskResult readSuccessfulTaskReport(Path dir, String fullName)
    {
        TaskResumeState resumeState;
        try {
            resumeState = mapper.readValue(dir.resolve(fullName + ".json").toFile(), TaskResumeState.class);
        }
        catch (FileNotFoundException ex) {
            return null;
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
        if (resumeState.getState() == TaskStateCode.SUCCESS) {
            return resumeState.getResult();
        }
        else {
            return null;
        }
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        backgroundUpdateAll();
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    void startUpdate(Path dir, StoredSessionAttemptWithSession attempt)
    {
        managedDirs.add(new ResumeStateDir(dir, attempt));
        startScheduleIfNotStarted();
    }

    private synchronized void startScheduleIfNotStarted()
    {
        if (executor == null) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("session-state-update-%d")
                    .build()
                    );
            executor.scheduleWithFixedDelay(() -> backgroundUpdateAll(), 1, 1, TimeUnit.SECONDS);
            this.executor = executor;
        }
    }

    private void backgroundUpdateAll()
    {
        Iterator<ResumeStateDir> ite = managedDirs.iterator();
        while (ite.hasNext()) {
            ResumeStateDir dir = ite.next();
            try {
                dir.update();
            }
            catch (Exception ex) {
                logger.error("Uncaught exception", ex);
                ite.remove();
            }
        }
    }

    private class ResumeStateDir
    {
        private final Path dir;
        private final StoredSessionAttemptWithSession attempt;
        private final Set<Long> doneTaskIdList = new HashSet<>();

        private ResumeStateDir(Path dir, StoredSessionAttemptWithSession attempt)
        {
            this.dir = dir;
            this.attempt = attempt;
        }

        public StoredSessionAttemptWithSession getSessionAttempt()
        {
            return attempt;
        }

        private void update()
        {
            List<ArchivedTask> tasks = sessionStoreManager
                .getSessionStore(attempt.getSiteId())
                .getTasksOfAttempt(attempt.getId());
            for (ArchivedTask task : tasks) {
                tryWriteStateFile(task);
            }
        }

        private void tryWriteStateFile(ArchivedTask task)
        {
            if (!Tasks.isDone(task.getState())) {
                return;
            }
            if (doneTaskIdList.contains(task.getId())) {
                return;
            }
            if (task.getState() == TaskStateCode.SUCCESS) {
                try {
                    writeStateFile(task);
                }
                catch (IOException ex) {
                    logger.error("Failed to write state file", ex);
                    return;
                }
            }
            doneTaskIdList.add(task.getId());
        }

        private void writeStateFile(ArchivedTask task)
            throws IOException
        {
            // grouping-only tasks don't have reports
            TaskResumeState state = TaskResumeState.of(
                    task.getFullName(),
                    task.getState(),
                    TaskResult.builder()
                        .subtaskConfig(task.getSubtaskConfig())
                        .exportParams(task.getExportParams())
                        .storeParams(task.getStoreParams())
                        .report(task.getReport().or(TaskReport.empty()))
                        .build());

            Files.write(dir.resolve(task.getFullName() + ".json"), mapper.writeValueAsBytes(state));
        }
    }
}
