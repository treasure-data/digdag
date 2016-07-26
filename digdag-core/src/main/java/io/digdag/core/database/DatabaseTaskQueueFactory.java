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
import io.digdag.core.database.DatabaseTaskQueueStore.LockResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
                        request.getSiteId(),
                        request.getQueueName(),
                        request.getPriority(),
                        request.getTaskId(),
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
        public List<TaskRequest> lockSharedTasks(int limit, String agentId, int lockSeconds, long maxWaitMillis)
        {
            ImmutableList.Builder<TaskRequest> builder = ImmutableList.builder();
            for (LockResult lock : store.lockSharedTasks(limit, agentId, lockSeconds)) {
                try {
                    byte[] data = store.getTaskData(lock.getLockId());
                    builder.add(decodeTask(data, lock));
                }
                catch (ResourceNotFoundException ex) {
                    continue;
                }
            }
            List<TaskRequest> result = builder.build();
            if (result.isEmpty() && maxWaitMillis >= 0) {
                sleepUntilEnqueue(sharedTaskSleepHelper, maxWaitMillis);
            }
            return result;
        }

        private void sleepUntilEnqueue(Object helper, long maxWaitMillis)
        {
            synchronized (helper) {
                try {
                    helper.wait(maxWaitMillis);
                }
                catch (InterruptedException ex) {
                    return;
                }
            }
        }

        @SuppressFBWarnings("NN_NAKED_NOTIFY")
        private void noticeEnqueue(Object helper)
        {
            synchronized (helper) {
                helper.notifyAll();
            }
        }

        @Override
        public void interruptLocalWait()
        {
            noticeEnqueue(sharedTaskSleepHelper);
            noticeEnqueue(taskSleepHelper);
        }

        @Override
        public void taskHeartbeat(int siteId, List<String> lockedIds, String agentId, int lockSeconds)
            throws TaskStateException
        {
            // TODO this is insecure because siteId is not checked
            for (String lockId : lockedIds) {
                LockResult lock = decodeLockId(lockId);
                boolean success = store.heartbeat(lock, agentId, lockSeconds);
                // TODO throw if not success?
            }
        }

        @Override
        public void delete(int siteId, String lockId, String agentId)
            throws TaskStateException
        {
            try {
                LockResult lock = decodeLockId(lockId);
                store.delete(siteId, lock, agentId);
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

        private TaskRequest decodeTask(byte[] data, LockResult lock)
        {
            try {
                return TaskRequest.withLockId(
                    mapper.readValue(data, TaskRequest.class),
                    encodeLockId(lock));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private String encodeLockId(LockResult lock)
        {
            if (lock.getSharedTask()) {
                return "s" + lock.getLockId();
            }
            else {
                return "k" + lock.getLockId();
            }
        }

        private LockResult decodeLockId(String encoded)
        {
            boolean sharedTask = encoded.startsWith("s");
            long lockId = Long.parseLong(encoded.substring(1));
            return LockResult.of(sharedTask, lockId);
        }
    }
}
