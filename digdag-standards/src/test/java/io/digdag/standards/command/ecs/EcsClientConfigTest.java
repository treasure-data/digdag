package io.digdag.standards.command.ecs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EcsClientConfigTest
{
    private ObjectMapper om = DigdagClient.objectMapper();
    private final ConfigFactory cf = new ConfigFactory(om);
    private Config systemConfig;

    @Before
    public void setUp()
    {
        systemConfig = cf.create()
                .set("agent.command_executor.ecs.cluster01.access_key_id", "default_access_key")
                .set("agent.command_executor.ecs.cluster01.secret_access_key", "default_secret_access_key")
                .set("custom_ecs_config.access_key_id", "custom_access_key")
                .set("custom_ecs_config.secret_access_key", "custom_secret_access_key");
    }

    @Test
    public void testCreateFromTaskConfigWithoutSystemConfigPrefix()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01"));

        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        assertEquals("default_access_key", ecsConfig.getAccessKeyId());
        assertEquals("default_secret_access_key", ecsConfig.getSecretAccessKey());
    }

    @Test
    public void testCreateFromTaskConfigWithSystemConfigPrefix()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                                .set("system_config_prefix", "custom_ecs_config."));

        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        assertEquals("custom_access_key", ecsConfig.getAccessKeyId());
        assertEquals("custom_secret_access_key", ecsConfig.getSecretAccessKey());
    }

    @Test
    public void testCreateFromTaskConfigWithValidPlacementStrategy()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                                .set("placement_strategy_type", "random"));

        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        assertEquals("random", ecsConfig.getPlacementStrategyType().get());
    }

    @Test
    public void testCreateFromTaskConfigWithValidPlacementStrategyAndType()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                                .set("placement_strategy_type", Optional.of("binpack"))
                                .set("placement_strategy_field", Optional.of("memory")));

        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        assertEquals("binpack", ecsConfig.getPlacementStrategyType().get());
        assertEquals("memory", ecsConfig.getPlacementStrategyField().get());
    }

    @Test
    public void testCreateFromTaskConfigWithValidPlacementStrategyTypeOnly()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                                .set("placement_strategy_field", Optional.of("memory")));

        try {
            EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
            fail("ConfigException was expected");
        }
        catch (Exception e) {
            assertTrue(e instanceof ConfigException);
            assertEquals("PlacementStrategyField must be set with PlacementStrategyType", e.getMessage());
        }
    }
}
