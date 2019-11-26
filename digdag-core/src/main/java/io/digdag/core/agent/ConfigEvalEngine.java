package io.digdag.core.agent;

import java.util.List;
import java.util.Map;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.regex.Pattern;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.Invocable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.metrics.DigdagMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import io.digdag.client.config.Config;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConfigEvalEngine
    implements TemplateEngine
{
    private static Logger logger = LoggerFactory.getLogger(ConfigEvalEngine.class);

    private static final String DIGDAG_JS_RESOURCE_PATH = "/io/digdag/core/agent/digdag.js";

    private static final String[] LIBRARY_JS_RESOURCE_PATHS = {
        "/io/digdag/core/agent/moment.min.js"
    };

    private static final List<String> RUNTIME_JS_CONTENTS;

    private static String readResource(String resourceName)
    {
        try (InputStream in = ConfigEvalEngine.class.getResourceAsStream(resourceName)) {
            return CharStreams.toString(new InputStreamReader(in, UTF_8));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    static {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        builder.add(readResource(DIGDAG_JS_RESOURCE_PATH));
        for (String lib : LIBRARY_JS_RESOURCE_PATHS) {
            builder.add(readResource(lib));
        }

        RUNTIME_JS_CONTENTS = builder.build();
    }

    private final ObjectMapper jsonMapper;
    private final NashornScriptEngineFactory jsEngineFactory;
    private DigdagMetrics metrics;

    @Inject
    public ConfigEvalEngine()
    {
        this.jsonMapper = new ObjectMapper();
        this.jsEngineFactory = new NashornScriptEngineFactory();
    }

    @DigdagTimed(value = "ceval_", category = "agent", appendMethodName = true)
    protected Config eval(Config config, Config params)
        throws TemplateException
    {
        ObjectNode object = config.convert(ObjectNode.class);
        ObjectNode built = new Context(params).evalObjectRecursive(object);
        return config.getFactory().create(built);
    }

    @DigdagTimed(value = "ceval_", category = "agent", appendMethodName = true)
    protected Invocable newTemplateInvocable(Config params)
    {
        ScriptEngine jsEngine = jsEngineFactory.getScriptEngine(new String[] {
            //"--language=es6",  // this is not even accepted with jdk1.8.0_20 and has a bug with jdk1.8.0_51
            "--no-java",
            "--no-syntax-extensions",
            "-timezone=" + params.get("timezone", String.class),
        });
        try {
            for (String runtimeJs : RUNTIME_JS_CONTENTS) {
                jsEngine.eval(runtimeJs);
            }
        }
        catch (ScriptException | ClassCastException ex) {
            throw new IllegalStateException("Unexpected script evaluation failure", ex);
        }
        return (Invocable) jsEngine;
    }

    @DigdagTimed(value = "ceval_", category = "agent", appendMethodName = true)
    protected String invokeTemplate(Invocable templateInvocable, String code, Config params)
        throws TemplateException
    {
        String context;
        try {
            context = jsonMapper.writeValueAsString(params);
        }
        catch (RuntimeException | IOException ex) {
            throw new TemplateException("Failed to serialize parameters to JSON", ex);
        }
        try {
            String evaluated = (String) templateInvocable.invokeFunction("template", code, context);
            return evaluated;
        }
        catch (ScriptException ex) {
            String message;
            if (ex.getCause() != null) {
                // ScriptException.getMessage includes filename and line number but they
                // are confusing because filename is always dummy file name and line number
                // is not accurate.
                message = ex.getCause().getMessage();
            }
            else {
                message = ex.getMessage();
            }
            throw new TemplateException("Failed to evaluate a variable " + code + " (" + message + ")");
        }
        catch (NoSuchMethodException ex) {
            throw new TemplateException("Failed to evaluate JavaScript code: " + code, ex);
        }
    }

    private class Context
    {
        private final Config params;
        private Invocable templateInvocable = null;
        private final ImmutableList<String> noEvaluatedKeys = ImmutableList.of("_do",  "_else_do");

        public Context(Config params)
        {
            this.params = params;
        }

        // ToDo Lombok Getter(lazy=true)
        private Invocable lazyGetTemplateInvocable()
        {
            if (templateInvocable == null) {
                synchronized(this) {
                    templateInvocable = newTemplateInvocable(params);
                }
            }
            return templateInvocable;
        }

        private ObjectNode evalObjectRecursive(ObjectNode local)
            throws TemplateException
        {
            ObjectNode built = local.objectNode();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(local.fields())) {
                JsonNode value = pair.getValue();
                JsonNode evaluated;
                if (noEvaluatedKeys.contains(pair.getKey())) {
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
            String resultText = null;
            if (requireInvokdetemplate(code)) {
                resultText = invokeTemplate(lazyGetTemplateInvocable(), code, scopedParams);
            }
            else {
                resultText = code;
            }
            if (resultText == null) {
                return jsonMapper.getNodeFactory().nullNode();
            }
            else {
                return jsonMapper.getNodeFactory().textNode(resultText);
            }
        }
    }

    private static final Pattern requireInvokdetemplatePattern = Pattern.compile("(?m)^.*[$].*$", Pattern.MULTILINE);

    @VisibleForTesting
    protected boolean requireInvokdetemplate(String code) {
        return code != null && requireInvokdetemplatePattern.matcher(code).find();
    }

    @Override
    public String template(String content, Config params)
        throws TemplateException
    {
        String resultText = null;
        if (requireInvokdetemplate(content)) {
            Invocable templateInvocable = newTemplateInvocable(params);
            resultText = invokeTemplate(templateInvocable, content, params);
        }
        else {
            resultText = content;
        }
        if (resultText == null) {
            return "";
        }
        else {
            return resultText;
        }
    }
}
