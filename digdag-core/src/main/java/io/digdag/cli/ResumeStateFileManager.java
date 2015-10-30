package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.PreDestroy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.*;

public class ResumeStateFileManager
{
    private static Logger logger = LoggerFactory.getLogger(ResumeStateFileManager.class);

    private final ConfigSourceFactory cf;
    private final SessionStoreManager sessionStoreManager;
    private final FileMapper mapper;
    private final Map<File, StoredSession> targets;
    private final YAMLFactory yaml = new YAMLFactory();

    private ScheduledExecutorService executor = null;

    @Inject
    private ResumeStateFileManager(ConfigSourceFactory cf, SessionStoreManager sessionStoreManager, FileMapper mapper)
    {
        this.cf = cf;
        this.sessionStoreManager = sessionStoreManager;
        this.mapper = mapper;
        this.targets = new ConcurrentHashMap<>();
    }

    @PreDestroy
    public synchronized void preDestroy()
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
        return Tasks.isDone(
                sessionStoreManager
                .getSessionStore(session.getSiteId())
                .getRootState(session.getId()));
    }
}
