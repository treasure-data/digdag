package io.digdag.core.agent;

import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskQueueClient;
import io.digdag.core.queue.TaskQueueServerManager;
import io.digdag.core.workflow.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public class InProcessTaskServerApi
    implements TaskServerApi
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);
    private final ToLongFunction<TaskQueueLock> CONV_FUNC_FROM_TASK_QUEUE_LOCK_TO_INT = tql -> {
        try {
            // Retried task's unique name has suffix `.r${retryCount}`.
            // See io.digdag.core.workflow.WorkflowExecutor.encodeUniqueQueuedTaskName
            String[] decodedPartsOfUniqueName = tql.getUniqueName().split("\\.");
            return Long.parseLong(decodedPartsOfUniqueName[0]);
        }
        catch (Throwable e) {
            logger.warn("Failed to convert TaskQueueLock.uniqueName to integer. The `uniqueName` will be handled as 0", e);
            return 0;
        }
    };

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
            int lockSeconds, long maxSleepMillis)
    {
        // Sort the list in order to avoid deadlock with other exclusive lock on multiple rows
        List<TaskQueueLock> locks = directQueueClient.lockSharedAgentTasks(count, agentId.toString(), lockSeconds, maxSleepMillis).stream()
                .sorted(Comparator.comparingLong(CONV_FUNC_FROM_TASK_QUEUE_LOCK_TO_INT))
                .collect(Collectors.toList());
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
