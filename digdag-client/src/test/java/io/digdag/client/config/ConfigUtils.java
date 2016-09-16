package io.digdag.client.config;

import static io.digdag.client.DigdagClient.objectMapper;

public class ConfigUtils
{
    private ConfigUtils()
    { }

    public static final ConfigFactory configFactory = new ConfigFactory(objectMapper());

    public static Config newConfig()
    {
        return configFactory.create();
    }
}
