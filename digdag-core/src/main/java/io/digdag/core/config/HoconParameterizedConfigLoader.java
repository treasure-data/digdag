package io.digdag.core.config;

import java.util.Map;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import com.typesafe.config.ConfigIncluder;
import com.typesafe.config.ConfigIncludeContext;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

public class HoconParameterizedConfigLoader
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ObjectMapper treeObjectMapper = new ObjectMapper();
    private final ObjectMapper dumper;
    private ConfigLoaderManager nested = null;

    @Inject
    public HoconParameterizedConfigLoader(ObjectMapper dumper)
    {
        this.dumper = dumper;
    }

    public void setNestedLoader(ConfigLoaderManager nested)
    {
        this.nested = nested;
    }

    public ConfigElement loadParameterizedFile(File file, Config params)
        throws IOException
    {
        com.typesafe.config.Config parsed =
            com.typesafe.config.ConfigFactory.parseFile(file,
                /*"{ include \"" + file + "\" }",*/
                ConfigParseOptions.defaults()
                );

        logger.debug("parsed hocon:\n---\n{}\n---", parsed);

        String json = parsed.root().render(ConfigRenderOptions.concise().setJson(true));

        JsonNode node = treeObjectMapper.readTree(json);
        return new ConfigElement(validateJsonNode(node));
    }

    private static ObjectNode validateJsonNode(JsonNode node)
    {
        if (!node.isObject()) {
            throw new RuntimeJsonMappingException("Expected object to load Config but got "+node);
        }
        return (ObjectNode) node;
    }

    //private class Includer
    //    implements ConfigIncluder
    //{
    //    private final Config params;

    //    public Includer(Config params)
    //    {
    //        this.params = params;
    //    }

    //    public ConfigIncluder withFallback(ConfigIncluder fallback)
    //    {
    //        // ignore fallback
    //        return this;
    //    }

    //    public ConfigObject include(ConfigIncludeContext context, String what)
    //    {
    //        if (nested == null) {
    //            throw new RuntimeException("include and load tags are not allowed in this context");
    //        }
    //        Config parsed = nested.loadParameterizedFile(new File(what), params);
    //        String json = dumper.writeValueAsString(parsed);
    //    }
    //}
}
