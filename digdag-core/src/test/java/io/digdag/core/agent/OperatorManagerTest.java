package io.digdag.core.agent;

import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretStoreManager;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

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

    private OperatorManager operatorManager;

    @Before
    public void setUp()
    {
        operatorManager = new OperatorManager(agentConfig, agentId, callback, workspaceManager, cf, evalEngine, registry, secretStoreManager);
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
}
