package io.digdag.core.config;

import java.util.Map;
import java.util.ArrayDeque;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

public class YamlConfigLoader
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ObjectMapper treeObjectMapper = new ObjectMapper();
    private final ConfigFactory cf;

    // TODO set charset and timezone

    @Inject
    public YamlConfigLoader(ConfigFactory cf)
    {
        this.cf = cf;
    }

    public ConfigElement loadFile(File file)
        throws IOException
    {
        try (FileInputStream in = new FileInputStream(file)) {
            String content = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
            return loadString(content);
        }
    }

    public ConfigElement loadString(String content)
        throws IOException
    {
        // here doesn't use jackson-dataformat-yaml so that snakeyaml calls Resolver
        // and Composer. See also YamlTagResolver.
        Yaml yaml = new Yaml(new SafeConstructor(), new Representer(), new DumperOptions(), new YamlTagResolver());
        ObjectNode object = normalizeValidateObjectNode(yaml.load(content));
        return ConfigElement.of(object);
    }

    public ConfigElement loadParameterizedFile(File file, Config params)
        throws IOException
    {
        return ConfigElement.of(loadParameterized(file.toPath(), params));
    }

    public ObjectNode loadParameterized(Path path, Config params)
        throws IOException
    {
        String content;
        try (InputStream in = Files.newInputStream(path)) {
            content = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
        }

        Yaml yaml = new Yaml(new YamlParameterizedConstructor(), new Representer(), new DumperOptions(), new YamlTagResolver());
        ObjectNode object = normalizeValidateObjectNode(yaml.load(content));

        Path includeDir = path.toAbsolutePath().getParent();
        if (includeDir == null) {
            throw new IllegalArgumentException("Loading file named '/' is invalid");
        }

        return new ParameterizeContext(includeDir, params).evalObjectRecursive(object);
    }

    private class ParameterizeContext
    {
        private final Path includeDir;
        private final Config params;

        private ParameterizeContext(Path includeDir, Config params)
        {
            this.includeDir = includeDir.toAbsolutePath().normalize();
            this.params = params;
        }

        private ObjectNode evalObjectRecursive(ObjectNode object)
            throws IOException
        {
            ObjectNode built = object.objectNode();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(object.fields())) {
                JsonNode value = pair.getValue();
                if (value.isObject()) {
                    built.set(pair.getKey(), evalObjectRecursive((ObjectNode) value));
                }
                else if (value.isArray()) {
                    built.set(pair.getKey(), evalArrayRecursive((ArrayNode) value));
                }
                else if (pair.getKey().startsWith("!include:")) {
                    // !include tag is converted to !include:<UUID> by YamlParameterizedConstructor.
                    // So, here actually includes the file and merges it to the parent object
                    String name;
                    if (value.isTextual()) {
                        name = value.textValue();
                    }
                    else {
                        name = value.toString();
                    }
                    ObjectNode included = include(name);
                    for (Map.Entry<String, JsonNode> merging : ImmutableList.copyOf(included.fields())) {
                        built.set(merging.getKey(), merging.getValue());
                    }
                }
                else {
                    built.set(pair.getKey(), value);
                }
            }
            return built;
        }

        private ArrayNode evalArrayRecursive(ArrayNode array)
            throws IOException
        {
            ArrayNode built = array.arrayNode();
            for (JsonNode value : array) {
                JsonNode evaluated;
                if (value.isObject()) {
                    evaluated = evalObjectRecursive((ObjectNode) value);
                }
                else if (value.isArray()) {
                    evaluated = evalArrayRecursive((ArrayNode) value);
                }
                else {
                    evaluated = value;
                }
                built.add(evaluated);
            }
            return built;
        }

        public ObjectNode include(String name)
            throws IOException
        {
            Path path = includeDir.resolve(name).toAbsolutePath().normalize();
            if (!path.toString().startsWith(includeDir.toString())) {
                throw new RuntimeException("file name must not include ..: " + name);
            }

            return loadParameterized(path, params);
        }
    }

    private ObjectNode normalizeValidateObjectNode(Object object)
        throws IOException
    {
        JsonNode node = treeObjectMapper.readTree(treeObjectMapper.writeValueAsString(object));
        if (!node.isObject()) {
            throw new RuntimeJsonMappingException("Expected object to load Config but got "+node);
        }
        return (ObjectNode) node;
    }
}
