package io.digdag.client.config;

import javax.inject.Inject;
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

    public Config create(Object other)
    {
        return create().set("_", other).getNested("_");
    }

    public Config wrap(ObjectNode object)
    {
        return new Config(objectMapper, object);
    }
}
