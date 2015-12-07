package io.digdag.core.spi.config;

import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ConfigFactory
{
    private final ObjectMapper objectMapper;

    @Inject
    public ConfigFactory(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    public Config create()
    {
        return new Config(objectMapper);
    }

    public Config create(ObjectNode object)
    {
        return new Config(objectMapper, object);
    }

    public Config create(Object other)
    {
        return create().set("_", other).getNested("_");
    }
}
