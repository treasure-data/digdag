package io.digdag.cli;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.*;

public class ResumeStateFileManager
{
    private final ExecutorService executor;
    private final ObjectMapper mapper;

    @Inject
    private ResumeStateFileManager(ObjectMapper mapper)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ressume-state-update-%d")
                .build()
                );
        this.mapper = mapper;
    }

    public boolean exists(File file)
    {
        return file.exists() && file.length() > 0;
    }

    public ResumeState read(File file)
            throws IOException
    {
        try (InputStream in = new FileInputStream(file)) {
            return mapper.readValue(in, ResumeState.class);
        }
    }

    public void syncOnJvmShutdown()
    {
        // TODO
    }

    public void startUpdate(File file,
            SessionStore sessionStore, StoredSession session)
    {
        // TODO
    }

    private static ResumeState getResumeState(SessionStore sessionStore, StoredSession session)
    {
        ImmutableMap.Builder<String, TaskReport> taskReports = ImmutableMap.builder();
        List<StoredTask> tasks = sessionStore.getTasks(session.getId(), Integer.MAX_VALUE, Optional.absent());  // TODO paging
        for (StoredTask task : tasks) {
            if (task.getReport().isPresent()) {
                taskReports.put(task.getFullName(), task.getReport().get());
            }
        }
        return ResumeState.builder()
            .reports(taskReports.build())
            .build();
    }
}
