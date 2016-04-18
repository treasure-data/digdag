package io.digdag.client.config;

import java.io.IOException;
import javax.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigFactory
{
    final ObjectMapper objectMapper;

    @Inject
    public ConfigFactory(ObjectMapper typeConverter)
    {
        this.objectMapper = typeConverter;
    }

    public Config create()
    {
        return new Config(objectMapper);
    }

    public Config create(Object other)
    {
        return create().set("_", other).getNested("_");
    }

    public Config fromJsonString(String json)
    {
        try {
            return new Config(objectMapper, objectMapper.readTree(json));
        }
        catch (IOException ex) {
            throw new ConfigException(ex);
        }
    }
}
