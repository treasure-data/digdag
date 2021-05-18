package io.digdag.core.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.Limits;
import io.digdag.core.workflow.OperatorTestingUtils;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.digdag.client.DigdagClient.objectMapper;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class OperatorManagerTest
{
    private AgentConfig agentConfig = AgentConfig.defaultBuilder().build();
    private AgentId agentId = AgentId.of("dummy");
    @Mock TaskCallbackApi callback;
    private ConfigFactory cf = new ConfigFactory(objectMapper());
    @Mock OperatorRegistry registry;
    @Mock SecretStoreManager secretStoreManager;
    private Limits limits = new Limits(cf.create());
    private Config simpleConfig = cf.fromJsonString("{\"echo>\":\"hello\"}");

    private OperatorManager operatorManager;

    @Before
    public void setUp()
    {
        ConfigEvalEngine evalEngine = new ConfigEvalEngine(
                ConfigUtils.newConfig()
                        .set("eval.js-engine-type", "graal")
                        .set("eval.extended-syntax", "false")
        );
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
    public void testRunWithHeartbeatWithCancelRequestedTask()
    {
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(simpleConfig).withIsCancelRequested(true);

        OperatorManager om = spy(operatorManager);
        Operator op = mock(Operator.class);
        OperatorFactory of = mock(OperatorFactory.class);
        doReturn(of).when(registry).get(any(), any());
        doReturn(op).when(of).newOperator(any());
        om.runWithHeartbeat(taskRequest);
        verify(op, times(0)).run();
        verify(op, times(1)).cleanup(any(TaskRequest.class));
        verify(callback, times(0)).taskSucceeded(any(), any(), any());
        verify(callback, times(1)).taskFailed(eq(taskRequest), any(), any());
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

    @Test
    public void evalConfigInMultiThreads()
            throws InterruptedException
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        Config taskConfig = ConfigUtils.configFactory.fromJsonString("{\n" +
                "  \"subtaskConfig\": {},\n" +
                "  \"exportParams\": {},\n" +
                "  \"resetStoreParams\": [],\n" +
                "  \"storeParams\": {},\n" +
                "  \"report\": {\n" +
                "    \"inputs\": [],\n" +
                "    \"outputs\": []\n" +
                "    },\n" +
                "  \"error\": {},\n" +
                "  \"resumingTaskId\": null,\n" +
                "  \"id\": 543210987,\n" +
                "  \"attemptId\": 234567890,\n" +
                "  \"upstreams\": [],\n" +
                "  \"updatedAt\": \"2021-05-17T10:06:24Z\",\n" +
                "  \"retryAt\": null,\n" +
                "  \"startedAt\": \"2021-05-17T06:11:34Z\",\n" +
                "  \"stateParams\": {\n" +
                "    \"job\": {\n" +
                "      \"jobId\": \"1234567890\",\n" +
                "      \"domainKey\": \"1234abcd-7654-bcde-ab98-0987fedcba21\",\n" +
                "      \"pollIteration\": null,\n" +
                "      \"errorPollIteration\": null\n" +
                "    }\n" +
                "  },\n" +
                "  \"retryCount\": 2,\n" +
                "  \"parentId\": 543210980,\n" +
                "  \"fullName\": \"+parent+mail^sub+create_data^sub+prepare1+step42\",\n" +
                "  \"config\": {\n" +
                "    \"local\": {\n" +
                "      \"td>\": \"create_data.sql\",\n" +
                "      \"create_table\": \"data_temp\"\n" +
                "    },\n" +
                "    \"export\": {}\n" +
                "  },\n" +
                "  \"taskType\": 0,\n" +
                "  \"state\": \"canceled\",\n" +
                "  \"stateFlags\": 9\n" +
                "},");
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(taskConfig);
        for (int i = 0; i < 10000; i++) {
            executorService.execute(() -> operatorManager.evalConfig(taskRequest));
        }
        executorService.shutdownNow();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)) {
            fail("Timeout");
        }
    }
}
