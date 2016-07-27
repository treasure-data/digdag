package io.digdag.standards.operator.pg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;

import java.io.IOException;
import java.util.Map;

public class ConfigBuilder
{
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new GuavaModule());
    private final YamlConfigLoader loader = new YamlConfigLoader();
    private final ConfigFactory configFactory = new ConfigFactory(mapper);

    public Config createConfig(Map<String, String> configInput)
            throws IOException
    {
        return loader.loadString(mapper.writeValueAsString(configInput)).toConfig(configFactory);
    }
}
