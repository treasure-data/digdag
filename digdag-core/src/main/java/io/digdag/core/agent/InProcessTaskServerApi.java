package io.digdag.core.agent;

import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.AccountRouting;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskQueueClient;
import io.digdag.core.queue.TaskQueueServerManager;
import io.digdag.core.workflow.WorkflowExecutor;
import java.util.List;

public class InProcessTaskServerApi
    implements TaskServerApi
{
    private final TaskQueueClient directQueueClient;
    private final WorkflowExecutor workflowExecutor;

    @Inject
    public InProcessTaskServerApi(
            TaskQueueServerManager queueManager,
            WorkflowExecutor workflowExecutor)
    {
        this.directQueueClient = queueManager.getInProcessTaskQueueClient();
        this.workflowExecutor = workflowExecutor;
    }

    @Override
    public List<TaskRequest> lockSharedAgentTasks(
            int count, AgentId agentId,
            int lockSeconds, long maxSleepMillis, AccountRouting accountRouting)
    {
        List<TaskQueueLock> locks = directQueueClient.lockSharedAgentTasks(count, agentId.toString(), lockSeconds, maxSleepMillis, accountRouting);
        if (locks.isEmpty()) {
            return ImmutableList.of();
        }
        else {
            return workflowExecutor.getTaskRequests(locks);
        }
    }

    @Override
    public void interruptLocalWait()
    {
        directQueueClient.interruptLocalWait();
    }
}
