package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.model.Tag;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EcsClientConfigBuilder
{
    private String clusterName;
    private String accessKeyId;
    private String secretAccessKey;
    private String region;
    private int maxRetries;
    private boolean assignPublicIp;
    private Optional<List<String>> subnets;
    private Optional<String> launchType;
    private Optional<String> capacityProviderName;
    private Optional<Integer> containerCpu;
    private Optional<Integer> containerMemory;
    private Optional<String> taskCpu;
    private Optional<String> taskMemory;
    private Optional<String> startedBy;
    private Optional<String> placementStrategyType;
    private Optional<String> placementStrategyField;
    private Optional<String> fargatePlatformVersion;
    private Optional<List<Tag>> tags;

    public EcsClientConfig build()
    {
        return new EcsClientConfig(this);
    }

    public EcsClientConfigBuilder withClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    public EcsClientConfigBuilder withLaunchType(Optional<String> launchType)
    {
        this.launchType = launchType;
        return this;
    }

    public EcsClientConfigBuilder withAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public EcsClientConfigBuilder withSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public EcsClientConfigBuilder withRegion(String region)
    {
        this.region = region;
        return this;
    }

    public EcsClientConfigBuilder withSubnets(Optional<String> subnets)
    {
        if (subnets.isPresent()) {
            this.subnets = Optional.of(Arrays.asList(subnets.get().split(",")));
        }
        else {
            this.subnets = Optional.absent();
        }
        return this;
    }

    public EcsClientConfigBuilder withMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public EcsClientConfigBuilder withCapacityProviderName(Optional<String> capacityProviderName)
    {
        this.capacityProviderName = capacityProviderName;
        return this;
    }

    public EcsClientConfigBuilder withContainerCpu(Optional<Integer> containerCpu)
    {
        this.containerCpu = containerCpu;
        return this;
    }

    public EcsClientConfigBuilder withContainerMemory(Optional<Integer> containerMemory)
    {
        this.containerMemory = containerMemory;
        return this;
    }

    public EcsClientConfigBuilder withStartedBy(Optional<String> startedBy)
    {
        this.startedBy = startedBy;
        return this;
    }

    public EcsClientConfigBuilder withAssignPublicIp(boolean assignPublicIp)
    {
        this.assignPublicIp = assignPublicIp;
        return this;
    }

    public EcsClientConfigBuilder withPlacementStrategyType(Optional<String> placementStrategyType)
    {
        this.placementStrategyType = placementStrategyType;
        return this;
    }

    public EcsClientConfigBuilder withPlacementStrategyField(Optional<String> placementStrategyField)
    {
        this.placementStrategyField = placementStrategyField;
        return this;
    }

    public EcsClientConfigBuilder withTaskCpu(Optional<String> taskCpu)
    {
        this.taskCpu = taskCpu;
        return this;
    }

    public EcsClientConfigBuilder withTaskMemory(Optional<String> taskMemory)
    {
        this.taskMemory = taskMemory;
        return this;
    }

    // see https://docs.aws.amazon.com/AmazonECS/latest/developerguide/platform_versions.html#platform-version-migration
    public EcsClientConfigBuilder withFargatePlatformVersion(Optional<String> fargatePlatformVersion)
    {
        this.fargatePlatformVersion = fargatePlatformVersion;
        return this;
    }

    public EcsClientConfigBuilder withTags(Optional<Config> tags)
    {
        if (tags.isPresent()) {
            final Config tagConfig = tags.get();
            final List<Tag> tagList = new ArrayList<>();
            for (String key : tagConfig.getKeys()) {
                final Tag t = new Tag()
                        .withKey(key)
                        .withValue(tagConfig.get(key, String.class));
                tagList.add(t);
            }
            this.tags = Optional.of(tagList);
        }
        else {
            this.tags = Optional.absent();
        }

        return this;
    }

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

    public Optional<Integer> getContainerCpu()
    {
        return containerCpu;
    }

    public Optional<Integer> getContainerMemory()
    {
        return containerMemory;
    }

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

    public Optional<String> getFargatePlatformVersion()
    {
        return fargatePlatformVersion;
    }

    public Optional<List<Tag>> getTags()
    {
        return tags;
    }
}
