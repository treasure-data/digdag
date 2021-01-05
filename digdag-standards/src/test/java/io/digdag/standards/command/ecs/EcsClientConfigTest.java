package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.model.Tag;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
                .set("agent.command_executor.ecs.cluster01.access_key_id", "cluster_specific_access_key")
                .set("agent.command_executor.ecs.cluster01.secret_access_key", "cluster_specific_secret_access_key")
                .set("agent.command_executor.ecs.__default_config__.access_key_id", "default_custom_access_key")
                .set("agent.command_executor.ecs.__default_config__.secret_access_key", "default_custom_secret_access_key");
    }

    @Test
    public void testCreateFromTaskConfigWithValidPlacementWithClusterSpecificConfig()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                                .set("placement_strategy_type", "random"));

        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        assertEquals("cluster_specific_access_key", ecsConfig.getAccessKeyId());
        assertEquals("cluster_specific_secret_access_key", ecsConfig.getSecretAccessKey());
    }

    @Test
    public void testCreateFromTaskConfigWithValidPlacementWithDefaultClusterSpecificConfig()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "unknown_cluster_name")
                                .set("placement_strategy_type", "random"));

        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        assertEquals("default_custom_access_key", ecsConfig.getAccessKeyId());
        assertEquals("default_custom_secret_access_key", ecsConfig.getSecretAccessKey());
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

    @Test
    public void testCreateFromSystemConfigWithFargatePlatformVersion()
    {
        Config sys = systemConfig.deepCopy();
        sys.set("agent.command_executor.ecs.name", "cluster01");
        sys.set("agent.command_executor.ecs.cluster01.fargate_platform_version", "1.4.0");
        sys.set("agent.command_executor.ecs.cluster01.region", "us-east-1");
        EcsClientConfig ecsConfig = EcsClientConfig.createFromSystemConfig(Optional.absent(), sys);
        assertEquals("1.4.0", ecsConfig.getFargatePlatformVersion().get());
    }

    @Test
    public void testCreateFromTaskConfigWithTags()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                                .set("tags", cf.create()
                                        .set("key1", "value1")
                                        .set("key2", "value2")
                                ));
        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        final Tag t1 = new Tag().withKey("key1").withValue("value1");
        final Tag t2 = new Tag().withKey("key2").withValue("value2");
        final List<Tag> expected = Arrays.asList(t1, t2);
        final List<Tag> actual = ecsConfig.getTags().get();
        assertTrue(actual.size() == expected.size());
        assertTrue(actual.containsAll(expected));
        assertTrue(expected.containsAll(actual));
    }

    @Test
    public void testCreateFromTaskConfigWithOutTags()
    {
        final Config taskConfig = cf.create()
                .set("agent.command_executor.ecs",
                        cf.create()
                                .set("region", "us-east-1")
                                .set("cluster_name", "cluster01")
                );
        final EcsClientConfig ecsConfig = EcsClientConfig.createFromTaskConfig(Optional.absent(), taskConfig, systemConfig);
        final Optional<List<Tag>> actual = ecsConfig.getTags();
        assertEquals(Optional.absent(), actual);
    }
}
