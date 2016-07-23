package io.digdag.core.workflow;

import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import io.digdag.core.agent.AgentId;
import io.digdag.core.repository.ResourceNotFoundException;

public interface TaskQueueDispatcher
{
    void dispatch(TaskRequest request)
        throws ResourceNotFoundException, TaskConflictException;

    void taskFinished(int siteId, String lockId, AgentId agentId)
        throws TaskConflictException, TaskNotFoundException;
}
