package io.digdag.core.agent;

import com.google.common.collect.ImmutableList;
import io.digdag.core.queue.TaskQueueServerManager;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskQueueLock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class InProcessTaskServerApiTest
{
    @Mock TaskQueueServerManager taskQueueServerManager;
    @Mock WorkflowExecutor workflowExecutor;
    @Mock TaskQueueClient taskQueueClient;
    private InProcessTaskServerApi taskServerApi;

    @Captor
    private ArgumentCaptor<List<TaskQueueLock>> taskQueueLockListCapture;

    @Before
    public void setUp()
    {
        doReturn(taskQueueClient).when(taskQueueServerManager).getInProcessTaskQueueClient();
        taskServerApi = new InProcessTaskServerApi(taskQueueServerManager, workflowExecutor);
    }

    @Test
    public void lockSharedAgentTasks()
    {
        TaskQueueLock taskQueueLock0 = mock(TaskQueueLock.class);
        doReturn("10").when(taskQueueLock0).getUniqueName();

        // This should be treated as id:0 to avoid throwing an exception
        TaskQueueLock taskQueueLock1 = mock(TaskQueueLock.class);
        doReturn("unknown").when(taskQueueLock1).getUniqueName();

        TaskQueueLock taskQueueLock2 = mock(TaskQueueLock.class);
        doReturn("1").when(taskQueueLock2).getUniqueName();

        // This should be treated as id:0 to avoid throwing an exception
        TaskQueueLock taskQueueLock3 = mock(TaskQueueLock.class);
        doReturn(null).when(taskQueueLock3).getUniqueName();

        TaskQueueLock taskQueueLock4 = mock(TaskQueueLock.class);
        doReturn("5").when(taskQueueLock4).getUniqueName();

        // Retried task's unique name has suffix `.r${retryCount}`.
        // See io.digdag.core.workflow.WorkflowExecutor.encodeUniqueQueuedTaskName
        TaskQueueLock taskQueueLock5 = mock(TaskQueueLock.class);
        doReturn("3.r8").when(taskQueueLock5).getUniqueName();

        // So the expected order is: 1 or 3 -> 1 or 3 -> 2 -> 5 -> 4 -> 0

        ImmutableList<TaskQueueLock> taskQueueLocks = ImmutableList.of(
                taskQueueLock0,
                taskQueueLock1,
                taskQueueLock2,
                taskQueueLock3,
                taskQueueLock4,
                taskQueueLock5);

        int fetchTaskCount = 42;
        String agentIdKey = "Hello";
        int lockSeconds = 3;
        long maxSleepMilli = 1234;

        doReturn(taskQueueLocks).when(taskQueueClient)
                .lockSharedAgentTasks(
                        eq(fetchTaskCount),
                        eq(agentIdKey),
                        eq(lockSeconds),
                        eq(maxSleepMilli));

        taskServerApi.lockSharedAgentTasks(fetchTaskCount, AgentId.of(agentIdKey),  lockSeconds, maxSleepMilli);

        verify(workflowExecutor, times(1)).getTaskRequests(taskQueueLockListCapture.capture());

        List<TaskQueueLock> taskQueueLockList = taskQueueLockListCapture.getValue();
        assertEquals(6, taskQueueLockList.size());
        assertTrue(taskQueueLockList.get(0) == taskQueueLock1 || taskQueueLockList.get(0) == taskQueueLock3);
        assertTrue(taskQueueLockList.get(1) == taskQueueLock1 || taskQueueLockList.get(1) == taskQueueLock3);
        assertEquals(taskQueueLock2, taskQueueLockList.get(2));
        assertEquals(taskQueueLock5, taskQueueLockList.get(3));
        assertEquals(taskQueueLock4, taskQueueLockList.get(4));
        assertEquals(taskQueueLock0, taskQueueLockList.get(5));
    }
}
