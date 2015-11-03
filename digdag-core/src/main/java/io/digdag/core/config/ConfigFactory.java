package io.digdag.core.config;

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

    public Config empty()
    {
        return new ConfigImpl(objectMapper);
    }

    public MutableConfig create()
    {
        return new ConfigImpl(objectMapper);
    }

    public MutableConfig create(ObjectNode object)
    {
        return new ConfigImpl(objectMapper, object);
    }

    public MutableConfig create(Object other)
    {
        return create().set("_", other).getNested("_");
    }
}
