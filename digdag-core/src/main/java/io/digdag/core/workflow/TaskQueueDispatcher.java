package io.digdag.core.workflow;

import io.digdag.spi.TaskRequest;
import io.digdag.core.agent.AgentId;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;

public interface TaskQueueDispatcher
{
    void dispatch(TaskRequest request)
        throws ResourceNotFoundException, ResourceConflictException;

    void taskFinished(int siteId, String lockId, AgentId agentId);
}
