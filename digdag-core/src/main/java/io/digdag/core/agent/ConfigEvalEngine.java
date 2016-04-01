package io.digdag.core.agent;

import java.util.Map;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.Charset;
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
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConfigEvalEngine
    implements TemplateEngine
{
    private static Logger logger = LoggerFactory.getLogger(ConfigEvalEngine.class);

    private static final String DIGDAG_JS_RESOURCE_PATH = "/digdag/agent/digdag.js";
    private static final String DIGDAG_JS;

    static {
        try (InputStream in = ConfigEvalEngine.class.getResourceAsStream(DIGDAG_JS_RESOURCE_PATH)) {
            DIGDAG_JS = CharStreams.toString(new InputStreamReader(in, UTF_8));
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

    protected Config eval(Path workspacePath, Config config, Config params)
        throws TemplateException
    {
        ObjectNode object = config.convert(ObjectNode.class);
        ObjectNode built = new Context(workspacePath, params).evalObjectRecursive(object);
        return config.getFactory().create(built);
    }

    private Invocable newTemplateInvocable(Config params)
    {
        ScriptEngine jsEngine = jsEngineFactory.getScriptEngine(new String[] {
            //"--language=es6",  // this is not even accepted with jdk1.8.0_20 and has a bug with jdk1.8.0_51
            "--no-java",
            "--no-syntax-extensions",
            "-timezone=" + params.get("timezone", String.class),
        });
        try {
            jsEngine.eval(DIGDAG_JS);
        }
        catch (ScriptException | ClassCastException ex) {
            throw new IllegalStateException("Unexpected script evaluation failure", ex);
        }
        return (Invocable) jsEngine;
    }

    private String invokeTemplate(Invocable templateInvocable, Path workspacePath, String code, Config params)
        throws TemplateException
    {
        try {
            String context = jsonMapper.writeValueAsString(params);
            return (String) templateInvocable.invokeFunction("template", code, context);
        }
        catch (ScriptException | NoSuchMethodException | IOException ex) {
            throw new TemplateException("Failed to evaluate JavaScript code: " + code, ex);
        }
    }

    private class Context
    {
        private final Path workspacePath;
        private final Config params;
        private final Invocable templateInvocable;

        public Context(Path workspacePath, Config params)
        {
            this.workspacePath = workspacePath;
            this.params = params;
            this.templateInvocable = newTemplateInvocable(params);
        }

        private ObjectNode evalObjectRecursive(ObjectNode local)
            throws TemplateException
        {
            ObjectNode built = local.objectNode();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(local.fields())) {
                JsonNode value = pair.getValue();
                JsonNode evaluated;
                if (pair.getKey().equals("_do")) {
                    // don't evaluate _do parameters
                    evaluated = value;
                }
                else if (value.isObject()) {
                    evaluated = evalObjectRecursive((ObjectNode) value);
                }
                else if (value.isArray()) {
                    evaluated = evalArrayRecursive(built, (ArrayNode) value);
                }
                else if (value.isTextual()) {
                    // eval using template engine
                    String code = value.textValue();
                    evaluated = evalValue(built, code);
                }
                else {
                    evaluated = value;
                }
                built.set(pair.getKey(), evaluated);
            }
            return built;
        }

        private ArrayNode evalArrayRecursive(ObjectNode local, ArrayNode array)
            throws TemplateException
        {
            ArrayNode built = array.arrayNode();
            for (JsonNode value : array) {
                JsonNode evaluated;
                if (value.isObject()) {
                    evaluated = evalObjectRecursive((ObjectNode) value);
                }
                else if (value.isArray()) {
                    evaluated = evalArrayRecursive(local, (ArrayNode) value);
                }
                else if (value.isTextual()) {
                    // eval using template engine
                    String code = value.textValue();
                    evaluated = evalValue(local, code);
                }
                else {
                    evaluated = value;
                }
                built.add(evaluated);
            }
            return built;
        }

        private JsonNode evalValue(ObjectNode local, String code)
            throws TemplateException
        {
            Config scopedParams = params.deepCopy();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(local.fields())) {
                scopedParams.set(pair.getKey(), pair.getValue());
            }
            String resultText = invokeTemplate(templateInvocable, workspacePath, code, scopedParams);
            if (resultText == null) {
                return jsonMapper.getNodeFactory().nullNode();
            }
            else {
                return jsonMapper.getNodeFactory().textNode(resultText);
            }
        }
    }

    @Override
    public String template(Path basePath, String content, Config params)
        throws TemplateException
    {
        Invocable templateInvocable = newTemplateInvocable(params);
        String resultText = invokeTemplate(templateInvocable, basePath, content, params);
        if (resultText == null) {
            return "";
        }
        else {
            return resultText;
        }
    }

    @Override
    public String templateFile(Path basePath, String fileName, Charset fileCharset, Config params)
        throws IOException, TemplateException
    {
        basePath = basePath.toAbsolutePath().normalize();
        Path absPath = basePath.resolve(fileName).normalize();
        if (!absPath.toString().startsWith(basePath.toString())) {
            throw new FileNotFoundException("file name must not include ..: " + fileName);
        }

        try (InputStream in = Files.newInputStream(absPath)) {
            String content = CharStreams.toString(new InputStreamReader(in, fileCharset));
            return template(basePath, content, params);
        }
    }
}
