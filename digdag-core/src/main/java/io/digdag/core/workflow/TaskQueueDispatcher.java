package io.digdag.core.workflow;

import com.google.common.base.Optional;
import io.digdag.spi.TaskQueueRequest;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import io.digdag.core.agent.AgentId;
import io.digdag.core.repository.ResourceNotFoundException;

public interface TaskQueueDispatcher
{
    void dispatch(int siteId, TaskQueueRequest request)
        throws ResourceNotFoundException, TaskConflictException;

    void taskFinished(int siteId, String lockId, AgentId agentId)
        throws TaskConflictException, TaskNotFoundException;

    boolean deleteInconsistentTask(String lockId);
}
