package io.digdag.core.database;

import java.util.List;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskQueue;
import io.digdag.spi.TaskQueueFactory;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskStateException;
import io.digdag.spi.TaskRequest;
import static io.digdag.core.queue.QueueSettingStore.DEFAULT_QUEUE_NAME;

public class DatabaseTaskQueueFactory
    implements TaskQueueFactory
{
    private final DatabaseTaskQueueStore store;
    private final ObjectMapper mapper;
    private final Object sharedTaskSleepHelper = new Object();
    private final Object taskSleepHelper = new Object();

    @Inject
    public DatabaseTaskQueueFactory(DatabaseTaskQueueStore store, ObjectMapper mapper)
    {
        this.store = store;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "database";
    }

    public TaskQueue getTaskQueue(Config systemConfig)
    {
        return new DatabaseTaskQueue();
    }

    public class DatabaseTaskQueue
        implements TaskQueue, TaskQueueServer
    {
        @Override
        public void enqueue(TaskRequest request)
            throws TaskStateException
        {
            try {
                store.enqueue(
                        request.getTaskInfo().getSiteId(),
                        request.getQueueName(),
                        request.getPriority(),
                        request.getTaskInfo().getId(),
                        encodeTask(request));
                if (request.getQueueName().equals(DEFAULT_QUEUE_NAME)) {
                    noticeEnqueue(sharedTaskSleepHelper);
                }
                else {
                    noticeEnqueue(taskSleepHelper);
                }
            }
            catch (ResourceConflictException ex) {
                throw new TaskStateException(ex);
            }
        }

        public TaskQueueServer getServer()
        {
            return this;
        }

        public TaskQueueClient getDirectClientIfSupported()
        {
            return null;
        }

        @Override
        public List<TaskRequest> lockSharedTasks(int limit, String agentId, int lockSeconds, long maxSleepMillis)
        {
            ImmutableList.Builder<TaskRequest> builder = ImmutableList.builder();
            for (long lockedTaskId : store.lockSharedTasks(limit, agentId, lockSeconds)) {
                try {
                    builder.add(decodeTask(store.getTaskData(lockedTaskId)));
                }
                catch (ResourceNotFoundException ex) {
                    continue;
                }
            }
            List<TaskRequest> result = builder.build();
            if (maxSleepMillis >= 0 && result.isEmpty()) {
                sleepUntilEnqueue(sharedTaskSleepHelper, maxSleepMillis);
            }
            return result;
        }

        private void sleepUntilEnqueue(Object helper, long maxSleepMillis)
        {
            synchronized (helper) {
                try {
                    helper.wait(maxSleepMillis);
                }
                catch (InterruptedException ex) {
                    return;
                }
            }
        }

        private void noticeEnqueue(Object helper)
        {
            synchronized (helper) {
                helper.notifyAll();
            }
        }

        @Override
        public void taskHeartbeat(int siteId, String queueName, long lockedTaskId, String agentId)
            throws TaskStateException
        {
            // TODO not implemented yet
        }

        @Override
        public void delete(int siteId, String queueName, long lockedTaskId, String agentId)
            throws TaskStateException
        {
            try {
                store.delete(siteId, queueName, lockedTaskId, agentId);
            }
            catch (ResourceNotFoundException | ResourceConflictException ex) {
                throw new TaskStateException(ex);
            }
        }

        private byte[] encodeTask(TaskRequest request)
        {
            try {
                return mapper.writeValueAsBytes(request);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private TaskRequest decodeTask(byte[] data)
        {
            try {
                return mapper.readValue(data, TaskRequest.class);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }
    }
}
