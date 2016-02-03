package io.digdag.core.agent;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalAgent
        implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(LocalAgent.class);

    private final ExecutorService executor;
    private final TaskQueue queue;
    private final TaskRunnerManager runner;
    private volatile boolean stop = false;

    public LocalAgent(TaskQueue queue, TaskRunnerManager runner)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("task-thread-%d")
                .build()
                );
        this.queue = queue;
        this.runner = runner;
    }

    public void stop()
    {
        stop = true;
    }

    public void shutdown()
    {
        executor.shutdown();
        // TODO wait for shutdown completion?
    }

    @Override
    public void run()
    {
        while (!stop) {
            try {
                Optional<TaskRequest> req = queue.receive(10_000);
                if (req.isPresent()) {
                    executor.submit(() -> {
                        try {
                            runner.run(req.get());
                        }
                        catch (Throwable t) {
                            logger.error("Uncaught exception. Task heartbeat for at-least-once task execution is not implemented yet.", t);
                        }
                    });
                }
            }
            catch (Throwable t) {
                logger.error("Uncaught exception", t);
            }
        }
    }
}
