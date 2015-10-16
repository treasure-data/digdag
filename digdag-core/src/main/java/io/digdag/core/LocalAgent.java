package io.digdag.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalAgent
        implements Runnable
{
    private final ExecutorService executor;
    private final TaskQueue queue;
    private final TaskRunner runner;

    public LocalAgent(TaskQueue queue, TaskRunner runner)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("local-agent-%d")
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
                Optional<Action> action = queue.receive(10_000);
                if (action.isPresent()) {
                    executor.submit(() -> {
                        try {
                            runner.run(action.get());
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
            System.err.println("Uncaught exception: "+t);
            t.printStackTrace(System.err);
        }
    }
}
