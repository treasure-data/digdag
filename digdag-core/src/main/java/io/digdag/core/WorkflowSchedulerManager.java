package io.digdag.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class WorkflowSchedulerManager
{
    private final ExecutorService executor;
    private final ScheduleStoreManager sm;
    private final SchedulerManager scheds;

    @Inject
    public WorkflowSchedulerManager(ScheduleStoreManager sm, SchedulerManager scheds)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scheduler-%d")
                .build()
                );
        this.sm = sm;
        this.scheds = scheds;
    }

    public void startScheduler()
    {
        executor.submit(new WorkflowScheduler(sm, scheds));
    }
}
