package io.digdag.standards.command.ecs;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.storage.StorageManager;

import java.util.List;

public class EcsClientConfig
{
    private static final String SYSTEM_CONFIG_PREFIX = "agent.command_executor.ecs.";
    public static final String TASK_CONFIG_ECS_KEY = "agent.command_executor.ecs";
    private static final int DEFAULT_MAX_TRIES = 3;

    public static EcsClientConfig of(final Optional<String> clusterName, final Config systemConfig, final Config taskConfig)
    {
        if (taskConfig.has(TASK_CONFIG_ECS_KEY)) {
            // from task config
            return createFromTaskConfig(clusterName, taskConfig);
        }
        else {
            // from system config
            return createFromSystemConfig(clusterName, systemConfig);
        }
    }

    public static EcsClientConfigBuilder builder()
    {
        return new EcsClientConfigBuilder();
    }

    public EcsClientConfig(EcsClientConfigBuilder builder)
    {
        this.clusterName = builder.getClusterName();
        this.launchType = builder.getLaunchType();
        this.accessKeyId = builder.getAccessKeyId();
        this.secretAccessKey = builder.getSecretAccessKey();
        this.region = builder.getRegion();
        this.subnets = builder.getSubnets();
        this.maxRetries = builder.getMaxRetries();
        this.capacityProviderName = builder.getCapacityProviderName();
        this.cpu = builder.getCpu();
        this.memory = builder.getMemory();
    }

    private static EcsClientConfig createFromTaskConfig(final Optional<String> clusterName, final Config config)
    {
        final String name;
        // `config` is assumed to have a nested config with following values
        // at the key of `TASK_CONFIG_ECS_KEY` from `config`.
        // - launch_type
        // - access_key_id
        // - secret_access_key
        // - region
        // - subnets (optional)
        // - max_retries (optional)
        // - capacity_provider_name (optional)
        // - memory (optional)
        // - cpu (optional)
        final Config ecsConfig = config.getNested(TASK_CONFIG_ECS_KEY);
        if (!clusterName.isPresent()) {
            // Throw ConfigException if 'name' doesn't exist in system config.
            name = ecsConfig.get("cluster_name", String.class);
        }
        else {
            name = clusterName.get();
        }

        return buildEcsClientConfig(name, ecsConfig);
    }

    private static EcsClientConfig createFromSystemConfig(final Optional<String> clusterName, final Config systemConfig)
    {
        final String name;
        if (!clusterName.isPresent()) {
            // Throw ConfigException if 'name' doesn't exist in system config.
            name = systemConfig.get(SYSTEM_CONFIG_PREFIX + "name", String.class); // ConfigException
        }
        else {
            name = clusterName.get();
        }

        final String extractedPrefix = SYSTEM_CONFIG_PREFIX + name + ".";
        final Config extracted = StorageManager.extractKeyPrefix(systemConfig, extractedPrefix);

        return buildEcsClientConfig(name, extracted);
    }

    private static EcsClientConfig buildEcsClientConfig(String clusterName, Config ecsConfig)
    {
        return EcsClientConfig.builder()
                .withClusterName(clusterName)
                .withLaunchType(ecsConfig.get("launch_type", String.class))
                .withAccessKeyId(ecsConfig.get("access_key_id", String.class))
                .withSecretAccessKey(ecsConfig.get("secret_access_key", String.class))
                .withRegion(ecsConfig.get("region", String.class))
                .withSubnets(ecsConfig.getOptional("subnets", String.class))
                .withMaxRetries(ecsConfig.get("max_retries", int.class, DEFAULT_MAX_TRIES))
                .withCapacityProviderName(ecsConfig.getOptional("capacity_provider_name", String.class))
                .withCpu(ecsConfig.getOptional("cpu", Integer.class))
                .withCpu(ecsConfig.getOptional("memory", Integer.class))
                .build();
    }

    private final String clusterName;
    private final String launchType;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String region;
    private final Optional<List<String>> subnets;
    private final int maxRetries;
    private final Optional<String> capacityProviderName;
    private final Optional<Integer> cpu;
    private final Optional<Integer> memory;

    public String getClusterName()
    {
        return clusterName;
    }

    public String getLaunchType()
    {
        return launchType;
    }

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    public String getRegion()
    {
        return region;
    }

    public Optional<List<String>> getSubnets()
    {
        return subnets;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    public Optional<String> getCapacityProviderName()
    {
        return capacityProviderName;
    }

    public Optional<Integer> getCpu() { return cpu; }

    public Optional<Integer> getMemory() { return memory; }
}
