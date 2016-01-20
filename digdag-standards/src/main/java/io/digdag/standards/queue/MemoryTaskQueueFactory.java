package io.digdag.standards.queue;

import java.util.Map;
import java.util.HashMap;
import java.util.Deque;
import java.util.ArrayDeque;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueue;
import io.digdag.spi.TaskQueueFactory;

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
        private Deque<TaskRequest> queue = new ArrayDeque<>();

        public MemoryTaskQueue(Config config)
        { }

        @Override
        public synchronized void put(TaskRequest request)
        {
            queue.offerLast(request);
            notifyAll();
        }

        @Override
        public synchronized Optional<TaskRequest> receive(long timeoutMillis)
                throws InterruptedException
        {
            if (queue.isEmpty()) {
                wait(timeoutMillis);
            }
            return Optional.fromNullable(queue.pollFirst());
        }
    }
}
