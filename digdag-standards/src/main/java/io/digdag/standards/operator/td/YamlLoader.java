package io.digdag.standards.operator.td;

import java.util.Map;
import java.util.ArrayDeque;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileNotFoundException;
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
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class YamlLoader
{
    private final ObjectMapper treeObjectMapper = new ObjectMapper();

    public ObjectNode loadString(String content)
        throws IOException
    {
        // here doesn't use jackson-dataformat-yaml so that snakeyaml calls Resolver
        // and Composer. See also YamlTagResolver.
        Yaml yaml = new Yaml(new SafeConstructor(), new Representer(), new DumperOptions(), new YamlTagResolver());
        ObjectNode object = normalizeValidateObjectNode(yaml.load(content));
        return object;
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
