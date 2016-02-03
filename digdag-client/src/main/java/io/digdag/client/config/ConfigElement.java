package io.digdag.client.config;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ConfigElement
{
    private final ObjectNode object;  // this is immutable

    @JsonCreator
    public ConfigElement(ObjectNode node)
    {
        this.object = node.deepCopy();
    }

    public Config toConfig(ConfigFactory factory)
    {
        // this is a optimization of factory.create(object)
        return new Config(factory.objectMapper, object.deepCopy());
    }

    public static ConfigElement copyOf(Config mutableConfig)
    {
        return new ConfigElement(mutableConfig.object.deepCopy());
    }

    @JsonValue
    @Deprecated  // this method is only for ObjectMapper
    public ObjectNode getObjectNode()
    {
        return object;
    }

    @Override
    public String toString()
    {
        return object.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Config)) {
            return false;
        }
        return object.equals(((Config) other).object);
    }

    @Override
    public int hashCode()
    {
        return object.hashCode();
    }
}
