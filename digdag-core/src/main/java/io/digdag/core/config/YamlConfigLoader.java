package io.digdag.core.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import org.yaml.snakeyaml.Yaml;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.google.inject.Inject;

public class YamlConfigLoader
{
    private final ObjectMapper treeObjectMapper = new ObjectMapper();
    private final ConfigFactory cf;

    @Inject
    public YamlConfigLoader(ConfigFactory cf)
    {
        this.cf = cf;
    }

    public Config load(InputStream in)
            throws IOException
    {
        JsonNode node = objectToJsonNode(new Yaml().load(in));
        return cf.create(validateJsonNode(node)).immutable();
    }

    public Config loadString(String content)
    {
        JsonNode node = objectToJsonNode(new Yaml().load(content));
        return cf.create(validateJsonNode(node)).immutable();
    }

    public Config loadFile(File file)
            throws IOException
    {
        try (FileInputStream in = new FileInputStream(file)) {
            return load(in);
        }
    }

    private JsonNode objectToJsonNode(Object object)
    {
        try {
            return treeObjectMapper.readTree(treeObjectMapper.writeValueAsString(object));
        }
        catch (IOException ex) {
            throw new RuntimeJsonMappingException(ex.toString());
        }
    }

    private static ObjectNode validateJsonNode(JsonNode node)
    {
        if (!node.isObject()) {
            throw new RuntimeJsonMappingException("Expected object to load Config but got "+node);
        }
        return (ObjectNode) node;
    }
}
