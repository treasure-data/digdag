package io.digdag.spi;

import java.util.List;
import java.util.function.Function;
import io.digdag.client.config.Config;

public interface TaskQueueServer
    extends TaskQueueClient
{
    // TODO multi-queue is not implemented yet.
    //   int createOrUpdateQueue(int queueId, Config config);
    //   void deleteQueueIfExists(int queueId);

    void enqueueSharedTask(TaskRequest request)
        throws TaskStateException;

    void enqueueQueueBoundTask(int queueId, TaskRequest request)
        throws TaskStateException;

    void deleteTask(int siteId, String lockId, String agentId)
        throws TaskStateException;
}
