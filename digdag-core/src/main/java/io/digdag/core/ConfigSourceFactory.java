package io.digdag.core;

import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ConfigSourceFactory
{
    private final ObjectMapper objectMapper;

    @Inject
    public ConfigSourceFactory(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    public ConfigSource create()
    {
        return new ConfigSource(objectMapper);
    }

    public ConfigSource create(ObjectNode object)
    {
        return new ConfigSource(objectMapper, object);
    }

    public ConfigSource create(Object other)
    {
        return create().set("_", other).getNested("_");
    }
}
