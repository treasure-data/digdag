package io.digdag.standards.command.ecs;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.storage.StorageManager;

import java.util.List;

public class EcsClientConfig
{
    private static final String SYSTEM_CONFIG_PREFIX = "agent.command_executor.ecs.";
    public static final String TASK_CONFIG_ECS_KEY = "agent.command_executor.ecs";
    private static final int DEFAULT_MAX_RETRIES = 3;

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
        this.startedBy = builder.getStartedBy();
        this.assignPublicIp = builder.isAssignPublicIp();
    }

    public static EcsClientConfig createFromTaskConfig(final Optional<String> clusterName, final Config taskConfig, final Config systemConfig)
    {
        final String name;
        // `taskConfig` is assumed to have a nested taskConfig with following values
        // at the key of `TASK_CONFIG_ECS_KEY` from `taskConfig`.
        // - launch_type (optional)
        // - region
        // - subnets (optional)
        // - max_retries (optional)
        // - capacity_provider_name (optional)
        // - memory (optional)
        // - cpu (optional)
        final Config ecsConfig = taskConfig.getNested(TASK_CONFIG_ECS_KEY).deepCopy();
        if (!clusterName.isPresent()) {
            // Throw ConfigException if 'name' doesn't exist in system ecsConfig.
            name = ecsConfig.get("cluster_name", String.class);
        }
        else {
            name = clusterName.get();
        }

        // This method assumes that `access_key_id` and `secret_access_key` are stored at `systemConfig`.
        ecsConfig.set("access_key_id", systemConfig.get(SYSTEM_CONFIG_PREFIX + name + ".access_key_id", String.class));
        ecsConfig.set("secret_access_key", systemConfig.get(SYSTEM_CONFIG_PREFIX + name + ".secret_access_key", String.class));

        return buildEcsClientConfig(name, ecsConfig);
    }

    public static EcsClientConfig createFromSystemConfig(final Optional<String> clusterName, final Config systemConfig)
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
                .withLaunchType(ecsConfig.getOptional("launch_type", String.class))
                .withAccessKeyId(ecsConfig.get("access_key_id", String.class))
                .withSecretAccessKey(ecsConfig.get("secret_access_key", String.class))
                .withRegion(ecsConfig.get("region", String.class))
                .withSubnets(ecsConfig.getOptional("subnets", String.class))
                .withMaxRetries(ecsConfig.get("max_retries", int.class, DEFAULT_MAX_RETRIES))
                .withCapacityProviderName(ecsConfig.getOptional("capacity_provider_name", String.class))
                .withCpu(ecsConfig.getOptional("cpu", Integer.class))
                .withMemory(ecsConfig.getOptional("memory", Integer.class))
                .withStartedBy(ecsConfig.getOptional("startedBy", String.class))
                // TODO removing default value.
                // This value was previously hard coded.
                // To keep consistency I once set the default value. But it should be removed after migration.
                .withAssignPublicIp(ecsConfig.get("assign_public_ip", boolean.class, true))
                .build();
    }

    private final String clusterName;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String region;
    private final int maxRetries;
    private boolean assignPublicIp;
    private final Optional<List<String>> subnets;
    private final Optional<String> launchType;
    private final Optional<String> capacityProviderName;
    private final Optional<Integer> cpu;
    private final Optional<Integer> memory;
    private final Optional<String> startedBy;

    public String getClusterName()
    {
        return clusterName;
    }

    public Optional<String> getLaunchType()
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

    public Optional<String> getStartedBy()
    {
        return startedBy;
    }

    public boolean isAssignPublicIp()
    {
        return assignPublicIp;
    }
}
