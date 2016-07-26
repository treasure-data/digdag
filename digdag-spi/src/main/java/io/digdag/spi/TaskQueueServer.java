package io.digdag.spi;

import java.util.List;
import java.util.function.Function;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;

public interface TaskQueueServer
    extends TaskQueueClient
{
    // TODO multi-queue is not implemented yet.
    //   int createOrUpdateQueue(int queueId, Optional<Integer> sharedSiteId, int maxConcurrency);
    //   void deleteQueueIfExists(int queueId);

    void enqueueDefaultQueueTask(int siteId, TaskQueueRequest request)
        throws TaskConflictException;

    void enqueueQueueBoundTask(int queueId, TaskQueueRequest request)
        throws TaskConflictException;

    void deleteTask(int siteId, String lockId, String agentId)
        throws TaskNotFoundException, TaskConflictException;

    boolean forceDeleteTask(String lockId);
}
