package io.digdag.core.agent;

import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.Limits;
import io.digdag.core.workflow.OperatorTestingUtils;
import io.digdag.spi.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OperatorManagerTest
{
    private AgentConfig agentConfig = AgentConfig.defaultBuilder().build();
    private AgentId agentId = AgentId.of("dummy");
    @Mock TaskCallbackApi callback;
    private ConfigFactory cf = new ConfigFactory(DigdagClient.objectMapper());
    @Mock OperatorRegistry registry;
    @Mock SecretStoreManager secretStoreManager;
    private Limits limits = new Limits(cf.create());
    private Config simpleConfig = cf.fromJsonString("{\"echo>\":\"hello\"}");

    private OperatorManager operatorManager;

    @Before
    public void setUp()
    {
        ConfigEvalEngine evalEngine = new ConfigEvalEngine(ConfigUtils.newConfig());
        WorkspaceManager workspaceManager = new LocalWorkspaceManager();

        operatorManager = new OperatorManager(
                agentConfig, agentId, callback, workspaceManager, cf,
                evalEngine, registry, secretStoreManager, limits);
    }

    @Test
    public void testFilterConfigForLogging()
            throws IOException
    {
        Config src = cf.fromJsonString(Resources.toString(OperatorManagerTest.class.getResource("/io/digdag/core/agent/operator_manager/filter_config_src.json"), UTF_8));
        Config srcBackup = src.deepCopy();
        Config expectedConfig = cf.fromJsonString(Resources.toString(OperatorManagerTest.class.getResource("/io/digdag/core/agent/operator_manager/filter_config_expected.json"), UTF_8));

        Config filteredConfig = operatorManager.filterConfigForLogging(src);

        assertEquals(src, srcBackup); // src must not be modified
        assertEquals(expectedConfig.getKeys().size(), filteredConfig.getKeys().size());
        for (String k : expectedConfig.getKeys()) {
            assertEquals(expectedConfig.get(k, Object.class), filteredConfig.get(k, Object.class));
        }
    }

    @Test
    public void testRunWithHeartbeatWithSuccessTask()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig);

        TaskResult result = mock(TaskResult.class);
        OperatorManager om = spy(operatorManager);
        doReturn(result).when(om).callExecutor(any(), any(), any());
        om.runWithHeartbeat(taskRequest);
        verify(callback, times(1)).taskSucceeded(eq(taskRequest), any(), eq(result));
        verify(callback, times(0)).taskFailed(any(), any(), any());
        verify(callback, times(0)).retryTask(any(), any(), anyInt(), any(), any());
    }

    @Test
    public void testRunWithHeartbeatWithFailedTask()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig);

        OperatorManager om = spy(operatorManager);
        doThrow(new TaskExecutionException("Zzz")).when(om).callExecutor(any(), any(), any());
        om.runWithHeartbeat(taskRequest);
        verify(callback, times(0)).taskSucceeded(any(), any(), any());
        verify(callback, times(1)).taskFailed(eq(taskRequest), any(), any());
        verify(callback, times(0)).retryTask(any(), any(), anyInt(), any(), any());
    }

    @Test
    public void testRunWithHeartbeatWithFailedTaskWithRetryableFailure()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig);

        OperatorManager om = spy(operatorManager);
        doThrow(TaskExecutionException.ofNextPolling(42, ConfigElement.empty()))
                .when(om)
                .callExecutor(any(), any(), any());
        om.runWithHeartbeat(taskRequest);
        verify(callback, times(0)).taskSucceeded(any(), any(), any());
        verify(callback, times(0)).taskFailed(any(), any(), any());
        verify(callback, times(1)).retryTask(eq(taskRequest), any(), eq(42), any(), any());
    }

    @Test
    public void testRunWithHeartbeatWithFailedTaskWithRuntimeException()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig);

        OperatorManager om = spy(operatorManager);
        doThrow(new RuntimeException("Zzz")).when(om).callExecutor(any(), any(), any());
        om.runWithHeartbeat(taskRequest);
        verify(callback, times(0)).taskSucceeded(any(), any(), any());
        verify(callback, times(1)).taskFailed(eq(taskRequest), any(), any());
        verify(callback, times(0)).retryTask(any(), any(), anyInt(), any(), any());
    }

    @Test
    public void testRunWithHeartbeatWithFailedTaskWithUnexpectedError()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig);

        OperatorManager om = spy(operatorManager);
        doThrow(new OutOfMemoryError("Zzz")).when(om).callExecutor(any(), any(), any());
        om.runWithHeartbeat(taskRequest);
        // In current implementation, OperatorManager does nothing and the task eventually will retried
        verify(callback, times(0)).taskSucceeded(any(), any(), any());
        verify(callback, times(0)).taskFailed(any(), any(), any());
        verify(callback, times(0)).retryTask(any(), any(), anyInt(), any(), any());
    }

    @Test
    public void testRunWithHeartbeatWithFailedTaskWithUnexpectedErrorButTheTaskShouldBeCanceled()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig)
                .withIsCancelRequested(true);

        OperatorManager om = spy(operatorManager);
        doThrow(new OutOfMemoryError("Zzz")).when(om).callExecutor(any(), any(), any());
        om.runWithHeartbeat(taskRequest);
        verify(callback, times(0)).taskSucceeded(any(), any(), any());
        verify(callback, times(1)).taskFailed(eq(taskRequest), any(), any());
        verify(callback, times(0)).retryTask(any(), any(), anyInt(), any(), any());
    }
}
