package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.io.File;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.workflow.Tasks;
import io.digdag.spi.TaskReport;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.repository.ResourceNotFoundException;

public class ResumeStateFileManager
{
    private static Logger logger = LoggerFactory.getLogger(ResumeStateFileManager.class);

    private final ConfigFactory cf;
    private final SessionStoreManager sessionStoreManager;
    private final FileMapper mapper;
    private final Map<File, StoredSession> targets;
    private final YAMLFactory yaml = new YAMLFactory();

    private ScheduledExecutorService executor = null;

    @Inject
    private ResumeStateFileManager(ConfigFactory cf, SessionStoreManager sessionStoreManager, FileMapper mapper)
    {
        this.cf = cf;
        this.sessionStoreManager = sessionStoreManager;
        this.mapper = mapper;
        this.targets = new ConcurrentHashMap<>();
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

    public void startUpdate(File file, StoredSession session)
    {
        targets.put(file, session);
        startScheduleIfNotStarted();
    }

    public void stopUpdate(File file)
    {
        targets.remove(file);
    }

    private synchronized void startScheduleIfNotStarted()
    {
        if (executor == null) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ressume-state-update-%d")
                    .build()
                    );
            executor.schedule(() -> backgroundUpdateAll(), 1, TimeUnit.SECONDS);
            this.executor = executor;
        }
    }

    private void backgroundUpdateAll()
    {
        Iterator<Map.Entry<File, StoredSession>> ite = targets.entrySet().iterator();
        while (ite.hasNext()) {
            Map.Entry<File, StoredSession> pair = ite.next();
            try {
                ResumeState resumeState = getResumeState(pair.getValue());
                mapper.writeFile(pair.getKey(), resumeState);
                if (isDone(pair.getValue())) {
                    ite.remove();
                }
            }
            catch (Exception ex) {
                logger.error("Uncaught exception", ex);
                ite.remove();
            }
        }
    }

    private ResumeState getResumeState(StoredSession session)
    {
        ImmutableMap.Builder<String, TaskReport> taskReports = ImmutableMap.builder();
        List<StoredTask> tasks = sessionStoreManager
            .getSessionStore(session.getSiteId())
            .getTasks(session.getId(), Integer.MAX_VALUE, Optional.absent());  // TODO paging
        for (StoredTask task : tasks) {
            if (task.getState() == TaskStateCode.SUCCESS) {
                taskReports.put(
                        task.getFullName(),
                        task.getReport().or(TaskReport.empty(cf)));  // grouping-only tasks don't have reports
            }
        }
        return ResumeState.of(taskReports.build());
    }

    private boolean isDone(StoredSession session)
    {
        try {
            return Tasks.isDone(
                    sessionStoreManager
                            .getSessionStore(session.getSiteId())
                            .getRootState(session.getId())
            );
        }
        catch (ResourceNotFoundException ex) {
            logger.warn("Session id={} is deleted. Assuming it is done.", session.getId());
            return true;
        }
    }
}
