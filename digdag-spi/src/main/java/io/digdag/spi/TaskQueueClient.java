package io.digdag.spi;

import java.util.List;

public interface TaskQueueClient
{
    List<TaskQueueLock> lockSharedAgentTasks(int count, String agentId, int lockSeconds, long maxSleepMillis);

    // TODO multi-queue is not implemented yet.
    //   List<TaskQueueLock> lockAgentBoundTasks(int queueId)

    List<String> taskHeartbeat(int siteId, List<String> lockedIds, String agentId, int lockSeconds);
}
