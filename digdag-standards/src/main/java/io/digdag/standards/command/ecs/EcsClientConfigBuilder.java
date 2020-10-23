package io.digdag.standards.command.ecs;

public class EcsClientConfigBuilder
{
    private String clusterName;
    private String launchType;
    private String accessKeyId;
    private String secretAccessKey;
    private String region;
    private String subnets;
    private int maxRetries;
    private String capacityProviderName;
    private int cpu;
    private int memory;

    public EcsClientConfig build()
    {
        return new EcsClientConfig(this);
    }

    public EcsClientConfigBuilder withClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    public EcsClientConfigBuilder withLaunchType(String launchType)
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

    public EcsClientConfigBuilder withSubnets(String subnets)
    {
        this.subnets = subnets;
        return this;
    }

    public EcsClientConfigBuilder withMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public EcsClientConfigBuilder withCapacityProviderName(String capacityProviderName)
    {
        this.capacityProviderName = capacityProviderName;
        return this;
    }

    public EcsClientConfigBuilder withCpu(int cpu)
    {
        this.cpu = cpu;
        return this;
    }

    public EcsClientConfigBuilder withMemory(int memory)
    {
        this.memory = memory;
        return this;
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

    public String getSubnets()
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

    public int getCpu()
    {
        return cpu;
    }

    public int getMemory()
    {
        return memory;
    }
}
