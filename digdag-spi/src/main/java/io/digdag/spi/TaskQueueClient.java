package io.digdag.spi;

import java.util.List;

public interface TaskQueueClient
{
    List<TaskRequest> lockSharedTasks(int count, String agentId, int lockSeconds, long maxSleepMillis);

    // TODO multi-queue is not implemented yet.
    //   List<TaskRequest> lockQueueBoundTasks(int queueId)

    void taskHeartbeat(int siteId, List<String> lockedIds, String agentId, int lockSeconds)
        throws TaskStateException;
}
