package io.digdag.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalAgentManager
{
    private final ExecutorService executor;
    private final TaskQueueManager queueManager;
    private final TaskRunner taskRunner;

    @Inject
    public LocalAgentManager(TaskQueueManager queueManager, TaskRunner taskRunner)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("local-agent-%d")
                .build()
                );
        this.queueManager = queueManager;
        this.taskRunner = taskRunner;
    }

    public void startLocalAgent(int siteId, String queueName)
    {
        executor.submit(new LocalAgent(
                    queueManager.getOrCreateTaskQueue(siteId, queueName),
                    taskRunner));
    }
}
