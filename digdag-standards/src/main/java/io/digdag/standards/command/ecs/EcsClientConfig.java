package io.digdag.standards.command.ecs;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.storage.StorageManager;

import java.util.Arrays;
import java.util.List;

public class EcsClientConfig
{
    private static final String SYSTEM_CONFIG_PREFIX = "agent.command_executor.ecs.";
    public static final String TASK_CONFIG_ECS_KEY = "agent.command_executor.ecs";

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
        this.subnets = Arrays.asList(builder.getSubnets().split(","));
        this.maxRetries = builder.getMaxRetries();
        this.capacityProviderName = builder.getCapacityProviderName();
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
        final Config ecsConfig = config.getNested(TASK_CONFIG_ECS_KEY);
        if (!clusterName.isPresent()) {
            // Throw ConfigException if 'name' doesn't exist in system config.
            name = ecsConfig.get("cluster_name", String.class);
        }
        else {
            name = clusterName.get();
        }

        return EcsClientConfig.builder()
                .withClusterName(name)
                .withLaunchType(ecsConfig.get("launch_type", String.class))
                .withAccessKeyId(ecsConfig.get("access_key_id", String.class))
                .withSecretAccessKey(ecsConfig.get("secret_access_key", String.class))
                .withRegion(ecsConfig.get("region", String.class))
                .withSubnets(ecsConfig.get("subnets", String.class, ""))
                .withMaxRetries(ecsConfig.get("max_retries", int.class, 3))
                .withCapacityProviderName(ecsConfig.get("capacity_provider_name", String.class, ""))
                .build();
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
        return new EcsClientConfig(name,
                extracted.get("launch_type", String.class),
                extracted.get("access_key_id", String.class),
                extracted.get("secret_access_key", String.class),
                extracted.get("region", String.class),
                extracted.get("subnets", String.class),
                extracted.get("max_retries", int.class, 3)
        );
    }

    private final String clusterName;
    private final String launchType;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String region;
    private final List<String> subnets;
    private final int maxRetries;
    private final String capacityProviderName;

    private EcsClientConfig(final String clusterName,
            final String launchType,
            final String accessKeyId,
            final String secretAccessKey,
            final String region,
            final String subnets,
            final int maxRetries)
    {
        this.clusterName = clusterName;
        this.launchType = launchType;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.region = region;
        this.subnets = Arrays.asList(subnets.split(",")); // TODO more robust
        this.maxRetries = maxRetries;
        this.capacityProviderName = "";
    }

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

    public List<String> getSubnets()
    {
        return subnets;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    public String getCapacityProviderName()
    {
        return capacityProviderName;
    }
}
