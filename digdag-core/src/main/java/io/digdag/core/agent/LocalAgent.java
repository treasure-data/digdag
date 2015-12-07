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

    @Override
    public void run()
    {
        try {
            while (true) {
                Optional<TaskRequest> req = queue.receive(10_000);
                if (req.isPresent()) {
                    executor.submit(() -> {
                        try {
                            runner.run(req.get());
                        }
                        catch (Throwable t) {
                            System.err.println("Uncaught exception: "+t);
                            t.printStackTrace(System.err);
                        }
                    });
                }
            }
        }
        catch (Throwable t) {
            logger.error("Uncaught exception", t);
        }
    }
}
