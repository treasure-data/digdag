package io.digdag.core.agent;

import java.util.Map;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.file.Path;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.Invocable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConfigEvalEngine
{
    private static Logger logger = LoggerFactory.getLogger(ConfigEvalEngine.class);

    private static final String DIGDAG_JS_RESOURCE_PATH = "/digdag/agent/digdag.js";
    private static final String DIGDAG_JS;

    static {
        try (InputStreamReader r = new InputStreamReader(ConfigEvalException.class.getResourceAsStream(DIGDAG_JS_RESOURCE_PATH), UTF_8)) {
            DIGDAG_JS = CharStreams.toString(r);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final ObjectMapper jsonMapper;
    private final NashornScriptEngineFactory jsEngineFactory;

    @Inject
    public ConfigEvalEngine()
    {
        this.jsonMapper = new ObjectMapper();
        this.jsEngineFactory = new NashornScriptEngineFactory();
    }

    protected Config eval(Path archivePath, Config config, Config params)
        throws ConfigEvalException
    {
        ObjectNode object = config.convert(ObjectNode.class);
        ObjectNode built = new Context(archivePath, params).evalObjectRecursive(object);
        return config.getFactory().create(built);
    }

    private class Context
    {
        private final Path archivePath;
        private final Config params;
        private final Invocable invocable;

        public Context(Path archivePath, Config params)
        {
            this.archivePath = archivePath;
            this.params = params;

            ScriptEngine jsEngine = jsEngineFactory.getScriptEngine(new String[] {
                "--language=es6",
                "--no-java",
                "--no-syntax-extensions",
                "-timezone=" + params.get("timezone", String.class),  // TODO is this working?
            });
            try {
                jsEngine.eval(DIGDAG_JS);
            }
            catch (ScriptException | ClassCastException ex) {
                throw new IllegalStateException("Unexpected script evaluation failure", ex);
            }
            this.invocable = (Invocable) jsEngine;
        }

        private ObjectNode evalObjectRecursive(ObjectNode local)
            throws ConfigEvalException
        {
            ObjectNode built = local.objectNode();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(local.fields())) {  // copy to prevent concurrent modification
                JsonNode value = pair.getValue();
                JsonNode evaluated;
                if (value.isTextual()) {
                    // eval using template engine
                    String code = value.textValue();
                    evaluated = evalValue(built, code);
                }
                else if (value.isObject()) {
                    evaluated = evalArrayRecursive(built, (ArrayNode) value);
                }
                else if (value.isArray()) {
                    evaluated = evalArrayRecursive(built, (ArrayNode) value);
                }
                else {
                    evaluated = value;
                }
                built.set(pair.getKey(), evaluated);
            }
            return built;
        }

        private ArrayNode evalArrayRecursive(ObjectNode local, ArrayNode array)
            throws ConfigEvalException
        {
            ArrayNode built = array.arrayNode();
            for (JsonNode value : array) {
                JsonNode evaluated;
                if (value.isTextual()) {
                    // eval using template engine
                    String code = value.textValue();
                    evaluated = evalValue(local, code);
                }
                else if (value.isObject()) {
                    evaluated = evalObjectRecursive((ObjectNode) value);
                }
                else if (value.isArray()) {
                    evaluated = evalArrayRecursive(local, (ArrayNode) value);
                }
                else {
                    evaluated = value;
                }
                built.add(evaluated);
            }
            return built;
        }

        private JsonNode evalValue(ObjectNode local, String code)
            throws ConfigEvalException
        {
            Config scopedParams = params.deepCopy();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(local.fields())) {
                scopedParams.set(pair.getKey(), pair.getValue());
            }
            return evalTemplate(archivePath, code, scopedParams);
        }

        private JsonNode evalTemplate(Path archivePath, String code, Config params)
            throws ConfigEvalException
        {
            try {
                String context = jsonMapper.writeValueAsString(params);
                String resultText = (String) invocable.invokeFunction("template", code, context);
                if (resultText == null) {
                    return jsonMapper.getNodeFactory().nullNode();
                }
                else {
                    return jsonMapper.getNodeFactory().textNode(resultText);
                }
            }
            catch (ScriptException | NoSuchMethodException | IOException ex) {
                throw new ConfigEvalException("Failed to evaluate JavaScript code: " + code, ex);
            }
        }
    }
}
