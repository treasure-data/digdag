package io.digdag.core;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalThreadTaskQueueDispatcher
        implements TaskQueueDispatcher
{
    private final ExecutorService pool;
    private final TaskApi api;
    private final ConfigSourceFactory cf;

    @Inject
    public LocalThreadTaskQueueDispatcher(TaskApi api, ConfigSourceFactory cf)
    {
        this.api = api;
        this.pool = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("local-queue-%d")
                .build()
                );
        this.cf = cf;
    }

    public void dispatch(Action action)
    {
        pool.submit(() -> {
            try {
                System.out.println("Running task: "+action.getFullName());
                System.out.println("  params: "+action.getParams());
                System.out.println("  stateParams: "+action.getStateParams());
                api.taskFinished(
                        action.getTaskId(),
                        action.getStateParams(),
                        cf.create(),
                        Optional.absent(),
                        Optional.absent(),
                        Optional.of(cf.create()),
                        Optional.of(
                            TaskReport.reportBuilder()
                            .carryParams(cf.create().set("done", new Date()))
                            .build()
                            ));
            }
            catch (Throwable t) {
                System.err.println("Uncaught exception: "+t);
                t.printStackTrace(System.err);
            }
        });
    }
}
