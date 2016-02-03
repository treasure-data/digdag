package io.digdag.client.config;

import java.util.Properties;
import java.util.Iterator;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ConfigElement
{
    public static ConfigElement copyOf(Config mutableConfig)
    {
        return new ConfigElement(mutableConfig.object.deepCopy());
    }

    @JsonCreator
    public static ConfigElement of(ObjectNode node)
    {
        return new ConfigElement(node.deepCopy());
    }

    public static ConfigElement empty()
    {
        return new ConfigElement(JsonNodeFactory.instance.objectNode());
    }

    private final ObjectNode object;  // this is immutable

    private ConfigElement(ObjectNode node)
    {
        this.object = node.deepCopy();
    }

    public Config toConfig(ConfigFactory factory)
    {
        // this is a optimization of factory.create(object)
        return new Config(factory.objectMapper, object.deepCopy());
    }

    public Properties toProperties()
    {
        Properties props = new Properties();
        setToPropertiesRecursive(props, "", object);
        return props;
    }

    private static void setToPropertiesRecursive(Properties props, String keyPrefix, ObjectNode object)
    {
        Iterator<Map.Entry<String, JsonNode>> ite = object.fields();
        while (ite.hasNext()) {
            Map.Entry<String, JsonNode> pair = ite.next();
            JsonNode value = pair.getValue();
            if (value.isObject()) {
                setToPropertiesRecursive(props, keyPrefix + pair.getKey() + ".", (ObjectNode) value);
            }
            else if (value.isTextual()) {
                props.put(keyPrefix + pair.getKey(), value.asText());
            }
            else {
                props.put(keyPrefix + pair.getKey(), value.toString());
            }
        }
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
