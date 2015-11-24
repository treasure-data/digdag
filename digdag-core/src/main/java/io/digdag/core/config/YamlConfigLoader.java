package io.digdag.core.config;

import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import com.google.common.base.Optional;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import org.yaml.snakeyaml.Yaml;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.loader.FileLocator;

public class YamlConfigLoader
{
    private final ObjectMapper treeObjectMapper = new ObjectMapper();
    private final ConfigFactory cf;

    @Inject
    public YamlConfigLoader(ConfigFactory cf)
    {
        this.cf = cf;
    }

    public Config loadFile(File file, Optional<File> resourceDirectory, Config renderParams)
            throws IOException
    {
        try (FileInputStream in = new FileInputStream(file)) {
            return load(in, resourceDirectory, renderParams);
        }
    }

    private Config load(InputStream in, Optional<File> resourceDirectory, Config renderParams)
            throws IOException
    {
        return loadString(
                CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8)),
                resourceDirectory, renderParams);
    }

    public Config loadString(String content, Optional<File> resourceDirectory, Config renderParams)
        throws FileNotFoundException
    {
        Jinjava jinjava = new Jinjava();

        Map<String, Object> bindings = new HashMap<>();
        for (String key : renderParams.getKeys()) {
            bindings.put(key, renderParams.get(key, Object.class));
        }

        if (resourceDirectory.isPresent()) {
            jinjava.setResourceLocator(new FileLocator(resourceDirectory.get()));
        }

        String template = jinjava.render(content, bindings);

        JsonNode node = objectToJsonNode(new Yaml().load(content));
        return cf.create(validateJsonNode(node));
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
