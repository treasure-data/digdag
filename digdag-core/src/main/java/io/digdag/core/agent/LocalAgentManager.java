package io.digdag.core.agent;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.queue.TaskQueueManager;

public class LocalAgentManager
{
    private final ExecutorService executor;
    private final TaskQueueManager queueManager;
    private final TaskRunnerManager taskRunnerManager;

    @Inject
    public LocalAgentManager(TaskQueueManager queueManager, TaskRunnerManager taskRunnerManager)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("local-agent-%d")
                .build()
                );
        this.queueManager = queueManager;
        this.taskRunnerManager = taskRunnerManager;
    }

    // TODO stop LocalAgent at @PreDestroy

    public void startLocalAgent(int siteId, String queueName)
    {
        executor.submit(
                new LocalAgent(
                    queueManager.getInProcessTaskQueueClient(siteId),
                    taskRunnerManager));
    }
}
