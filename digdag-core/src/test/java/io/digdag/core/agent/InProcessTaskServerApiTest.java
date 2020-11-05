package io.digdag.core.agent;

import com.google.common.collect.ImmutableList;
import io.digdag.core.queue.TaskQueueServerManager;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.ImmutableTaskQueueLock;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class InProcessTaskServerApiTest
{
    @Mock TaskQueueServerManager taskQueueServerManager;
    @Mock WorkflowExecutor workflowExecutor;
    @Mock TaskQueueClient taskQueueClient;
    private InProcessTaskServerApi taskServerApi;

    @Before
    public void setUp()
    {
        taskServerApi = new InProcessTaskServerApi(taskQueueServerManager, workflowExecutor);
    }

    @Test
    public void lockSharedAgentTasks()
    {
        TaskQueueLock taskQueueLock0 = mock(TaskQueueLock.class);
        doReturn("10").when(taskQueueLock0).getUniqueName();

        TaskQueueLock taskQueueLock1 = mock(TaskQueueLock.class);
        doReturn("unknown").when(taskQueueLock1).getUniqueName();

        TaskQueueLock taskQueueLock2 = mock(TaskQueueLock.class);
        doReturn("1").when(taskQueueLock2).getUniqueName();

        TaskQueueLock taskQueueLock3 = mock(TaskQueueLock.class);
        doReturn(null).when(taskQueueLock3).getUniqueName();

        TaskQueueLock taskQueueLock4 = mock(TaskQueueLock.class);
        doReturn("5").when(taskQueueLock4).getUniqueName();

        ImmutableList<TaskQueueLock> taskQueueLocks =
                ImmutableList.of(taskQueueLock0, taskQueueLock1, taskQueueLock2, taskQueueLock3, taskQueueLock4);

        int expectedCount = 42;
        AgentId expectedAgentId = AgentId.of("Hello");

        doReturn(taskQueueLocks).when(taskQueueClient)
                .lockSharedAgentTasks(
                        eq(expectedCount),
                        eq(expectedAgentId),
                        )
        doReturn(taskQueueClient).when(taskQueueServerManager).getInProcessTaskQueueClient();

        List<TaskRequest> taskRequests = taskServerApi.lockSharedAgentTasks(10, AgentId.of("hello"), 42, 1234);
    }
}
