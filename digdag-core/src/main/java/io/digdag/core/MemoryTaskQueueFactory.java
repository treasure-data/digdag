package io.digdag.core;

import java.util.Map;
import java.util.HashMap;
import java.util.Deque;
import java.util.ArrayDeque;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.config.Config;

public class MemoryTaskQueueFactory
        implements TaskQueueFactory
{
    private Map<String, MemoryTaskQueue> taskQueues;

    public MemoryTaskQueueFactory()
    {
        this.taskQueues = new HashMap<>();
    }

    @Override
    public String getType()
    {
        return "memory";
    }

    @Override
    public TaskQueue getTaskQueue(int siteId, String name, Config config)
    {
        String key = "" + siteId + ":" + name;
        synchronized (taskQueues) {
            MemoryTaskQueue queue = taskQueues.get(key);
            if (queue != null) {
                return queue;
            }
            queue = new MemoryTaskQueue(config);
            taskQueues.put(key, queue);
            return queue;
        }
    }

    private static class MemoryTaskQueue
            implements TaskQueue
    {
        private Deque<Action> queue = new ArrayDeque<>();

        public MemoryTaskQueue(Config config)
        { }

        @Override
        public synchronized void put(Action action)
        {
            queue.offerLast(action);
            notifyAll();
        }

        @Override
        public synchronized Optional<Action> receive(long timeoutMillis)
                throws InterruptedException
        {
            if (queue.isEmpty()) {
                wait(timeoutMillis);
            }
            return Optional.fromNullable(queue.pollFirst());
        }
    }
}
