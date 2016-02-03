package io.digdag.client.config;

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
}
