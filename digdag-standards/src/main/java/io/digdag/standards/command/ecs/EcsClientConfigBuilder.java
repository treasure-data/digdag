package io.digdag.standards.command.ecs;

import com.google.common.base.Optional;

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
    private Optional<Integer> cpu;
    private Optional<Integer> memory;
    private Optional<String> startedBy;
    private Optional<String> placementStrategyType;
    private Optional<String> placementStrategyField;

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

    public EcsClientConfigBuilder withCpu(Optional<Integer> cpu)
    {
        this.cpu = cpu;
        return this;
    }

    public EcsClientConfigBuilder withMemory(Optional<Integer> memory)
    {
        this.memory = memory;
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

    public Optional<Integer> getCpu()
    {
        return cpu;
    }

    public Optional<Integer> getMemory()
    {
        return memory;
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
}
