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

    public static EcsClientConfig of(final Optional<String> clusterName, final Config systemConfig, final Config taskConfig)
    {
        return EcsClientConfig.createFromSystemConfig(clusterName, systemConfig);
        /**
        if (config.has("ecs")) {
            // from task config
            return createFromTaskConfig(clusterName, config); // TODO
        }
        else {
            // from system config
            return EcsClientConfig.createFromSystemConfig(clusterName, systemConfig);
        }
         */
    }

    private static EcsClientConfig createFromTaskConfig(final Optional<String> clusterName, final Config config)
    {
        // TODO
        // We'd better to customize cluster config by task config
        throw new ConfigException("Not supported yet");
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
                extracted.get("max_retries", Integer.class)
        );
    }

    private final String clusterName;
    private final String launchType;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String region;
    private final List<String> subnets;
    private final Integer maxRetries;

    private EcsClientConfig(final String clusterName,
            final String launchType,
            final String accessKeyId,
            final String secretAccessKey,
            final String region,
            final String subnets,
            final Integer maxRetries)
    {
        this.clusterName = clusterName;
        this.launchType = launchType;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.region = region;
        this.subnets = Arrays.asList(subnets.split(",")); // TODO more robust
        this.maxRetries = maxRetries;
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

    public Integer getMaxRetries() { return maxRetries; }
}
