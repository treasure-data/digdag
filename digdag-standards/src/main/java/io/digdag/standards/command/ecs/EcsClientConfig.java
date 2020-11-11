package io.digdag.standards.command.ecs;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
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
        this.containerCpu = builder.getContainerCpu();
        this.containerMemory = builder.getContainerMemory();
        this.startedBy = builder.getStartedBy();
        this.assignPublicIp = builder.isAssignPublicIp();
        this.placementStrategyType = builder.getPlacementStrategyType();
        this.placementStrategyField = builder.getPlacementStrategyField();
        this.taskCpu = builder.getTaskCpu();
        this.taskMemory = builder.getTaskMemory();

        // All PlacementStrategyFields must be used with a PlacementStrategyType.
        // But some PlacementStrategyTypes can be used without any PlacementStrategyFields.
        // https://github.com/aws/aws-sdk-java/blob/1.11.686/aws-java-sdk-ecs/src/main/java/com/amazonaws/services/ecs/model/PlacementStrategy.java#L44-L52
        if (!placementStrategyType.isPresent() && placementStrategyField.isPresent()) {
            throw new ConfigException("PlacementStrategyField must be set with PlacementStrategyType");
        }
    }

    public static EcsClientConfig createFromTaskConfig(final Optional<String> clusterName, final Config taskConfig, final Config systemConfig)
    {
        final String name;
        // `taskConfig` is assumed to have a nested taskConfig with following values
        // at the key of `TASK_CONFIG_ECS_KEY` from `taskConfig`.
        // - region (String)
        // - system_config_prefix (optional/String)
        // - launch_type (optional/String)
        // - subnets (optional/String)
        // - max_retries (optional/int)
        // - capacity_provider_name (optional/String)
        // - container_memory (optional/Integer)
        // - container_cpu (optional/Integer)
        // - placementStrategyType (optional/String)
        // - placementStrategyField (optional/String)
        // - task_cpu (optional/String) e.g. `1 vcpu` or `1024` (CPU unit)
        // - task_memory (optional/String) e.g. `1 GB` or `1024` (MiB)
        // For more detail of the value format of `task_cpu` and `task_memory`, please see
        // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#task_size
        final Config ecsConfig = taskConfig.getNested(TASK_CONFIG_ECS_KEY).deepCopy();
        if (!clusterName.isPresent()) {
            // Throw ConfigException if 'name' doesn't exist in system ecsConfig.
            name = ecsConfig.get("cluster_name", String.class);
        }
        else {
            name = clusterName.get();
        }

        String systemConfigPrefix;
        if (ecsConfig.has("system_config_prefix")) {
            systemConfigPrefix = ecsConfig.get("system_config_prefix", String.class);
        }
        else {
            systemConfigPrefix = SYSTEM_CONFIG_PREFIX + name + ".";
        }

        // This method assumes that `access_key_id` and `secret_access_key` are stored at `systemConfig`.
        ecsConfig.set("access_key_id", systemConfig.get(systemConfigPrefix + "access_key_id", String.class));
        ecsConfig.set("secret_access_key", systemConfig.get(systemConfigPrefix + "secret_access_key", String.class));

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
                .withContainerCpu(ecsConfig.getOptional("container_cpu", Integer.class))
                .withContainerMemory(ecsConfig.getOptional("container_memory", Integer.class))
                .withStartedBy(ecsConfig.getOptional("startedBy", String.class))
                // TODO removing default value.
                // This value was previously hard coded.
                // To keep consistency I once set the default value. But it should be removed after migration.
                .withAssignPublicIp(ecsConfig.get("assign_public_ip", boolean.class, true))
                .withPlacementStrategyType(ecsConfig.getOptional("placement_strategy_type", String.class))
                .withPlacementStrategyField(ecsConfig.getOptional("placement_strategy_field", String.class))
                .withTaskCpu(ecsConfig.getOptional("task_cpu", String.class))
                .withTaskMemory(ecsConfig.getOptional("task_memory", String.class))
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
    private final Optional<Integer> containerCpu;
    private final Optional<Integer> containerMemory;
    private final Optional<String> taskCpu;
    private final Optional<String> taskMemory;
    private final Optional<String> startedBy;
    // In aws-sdk 1.11.686, only `random`, `spread`, and `binpack` are supported.
    // https://github.com/aws/aws-sdk-java/blob/1.11.686/aws-java-sdk-ecs/src/main/java/com/amazonaws/services/ecs/model/PlacementStrategyType.java#L23-L25
    private final Optional<String> placementStrategyType;
    // Available values are defined for each `placementStrategyType`.
    // E.g. For the `binpack` placement strategy, valid values are `cpu` and `memory`.
    // https://github.com/aws/aws-sdk-java/blob/1.11.686/aws-java-sdk-ecs/src/main/java/com/amazonaws/services/ecs/model/PlacementStrategy.java#L44-L52
    private final Optional<String> placementStrategyField;

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

    public Optional<Integer> getContainerCpu() { return containerCpu; }

    public Optional<Integer> getContainerMemory() { return containerMemory; }

    public Optional<String> getStartedBy()
    {
        return startedBy;
    }

    public boolean isAssignPublicIp()
    {
        return assignPublicIp;
    }

    public Optional<String> getPlacementStrategyType()
    {
        return placementStrategyType;
    }

    public Optional<String> getPlacementStrategyField()
    {
        return placementStrategyField;
    }

    public Optional<String> getTaskCpu()
    {
        return taskCpu;
    }

    public Optional<String> getTaskMemory()
    {
        return taskMemory;
    }
}
