package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.Limits;
import io.digdag.core.workflow.OperatorTestingUtils;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OperatorManagerTest
{
    private AgentConfig agentConfig = AgentConfig.defaultBuilder().build();
    private AgentId agentId = AgentId.of("dummy");
    @Mock TaskCallbackApi callback;
    @Mock WorkspaceManager workspaceManager;
    private ConfigFactory cf = new ConfigFactory(DigdagClient.objectMapper());
    @Mock ConfigEvalEngine evalEngine;
    @Mock OperatorRegistry registry;
    @Mock SecretStoreManager secretStoreManager;
    Limits limits = new Limits(cf.create());

    private OperatorManager operatorManager;

    @Before
    public void setUp()
    {
        operatorManager = new OperatorManager(agentConfig, agentId, callback, workspaceManager, cf, evalEngine, registry, secretStoreManager, limits);
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
    public void testRunWithHeartbeat() throws IOException {
        Config config = cf.fromJsonString("{\"echo>\":\"hello\"}");

        ConfigEvalEngine evalEngine = new ConfigEvalEngine(ConfigUtils.newConfig());
        TaskRequest taskRequest = OperatorTestingUtils.newTaskRequest(config);
        WorkspaceManager workspaceManager = new LocalWorkspaceManager();
        when(callback.openArchive(any())).thenReturn(Optional.absent());
        OperatorManager operatorManager = new OperatorManager(
                agentConfig, agentId, callback, workspaceManager, cf,
                evalEngine, registry, secretStoreManager, limits);

        OperatorManager om = spy(operatorManager);
        om.runWithHeartbeat(taskRequest);
    }
}
