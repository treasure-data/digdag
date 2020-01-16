package io.digdag.core.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConfigEvalEngine
    implements TemplateEngine
{
    private static Logger logger = LoggerFactory.getLogger(ConfigEvalEngine.class);

    // array of (name, resourceName)
    private static final String[][] LIBRARY_JS_RESOURCES = {
        new String[] { "digdag.js", "/io/digdag/core/agent/digdag.js", "digdag.js" },
        new String[] { "moment.min.js", "/io/digdag/core/agent/moment.min.js", "moment.min.js" },
    };

    // array of (name, code)
    static final String[][] LIBRARY_JS_CONTENTS;

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
        LIBRARY_JS_CONTENTS = new String[LIBRARY_JS_RESOURCES.length][];
        for (int i = 0; i < LIBRARY_JS_RESOURCES.length; i++) {
            LIBRARY_JS_CONTENTS[i] = new String[] {
                LIBRARY_JS_RESOURCES[i][0],
                readResource(LIBRARY_JS_RESOURCES[i][1])
            };
        }
    }

    private static ImmutableList<String> NO_EVALUATE_PARAMETERS = ImmutableList.of("_do",  "_else_do");

    private static enum JsEngineType
    {
        NASHORN("nashorn"),
        GRAAL("graal"),
        NASHORN_GRAAL_CHECK("nashorn-graal-check");

        final String configName;

        JsEngineType(String configName)
        {
            this.configName = configName;
        }
    }

    private static JsEngineType parseJsEngineType(String type)
    {
        switch (type) {
        case "nashorn":
            return JsEngineType.NASHORN;
        case "graal":
            return JsEngineType.GRAAL;
        case "nashorn-graal-check":
            return JsEngineType.NASHORN_GRAAL_CHECK;
        default:
            throw new ConfigException("Parameter 'eval.js-engine-type' must be either of nashorn, graal, or nashorn-graal-check but got " + type);
        }
    }

    @VisibleForTesting
    static JsEngineType defaultJsEngineType()
    {
        // Return NASHORN if runtime Java VM is older than 11. Otherwise,
        // by default, return GRAAL. This is because org.graalvm.js:js depends
        // on JVMCI (JEP 243) which is available only from Java 11.
        String javaSpecVer = System.getProperty("java.specification.version");
        int major = Integer.parseInt(javaSpecVer.split("[^0-9]")[0]);
        if (major < 11) {
            return JsEngineType.NASHORN;
        }
        return JsEngineType.GRAAL;
    }

    private final JsEngineType jsEngineType;
    private final NashornJsEngine nashorn;
    private final GraalJsEngine graal;
    private final ObjectMapper jsonMapper;

    @Inject
    public ConfigEvalEngine(Config systemConfig)
    {
        this(systemConfig.getOptional("eval.js-engine-type", String.class)
                .transform(type -> parseJsEngineType(type))
                .or(() -> defaultJsEngineType()));
    }

    @VisibleForTesting
    ConfigEvalEngine(JsEngineType jsEngineType)
    {
        logger.debug("Using JavaScript engine: {}", jsEngineType.configName);
        this.jsEngineType = jsEngineType;
        switch (jsEngineType) {
        case NASHORN:
            this.nashorn = new NashornJsEngine();
            this.graal = null;
            break;
        case GRAAL:
            this.nashorn = null;
            this.graal = new GraalJsEngine();
            break;
        case NASHORN_GRAAL_CHECK:
            this.nashorn = new NashornJsEngine();
            this.graal = new GraalJsEngine();
            break;
        default:
            throw new UnsupportedOperationException();
        }
        this.jsonMapper = new ObjectMapper();
    }

    interface JsEngine
    {
        interface Evaluator extends AutoCloseable
        {
            String evaluate(String code, Config scopedParams, ObjectMapper jsonMapper) throws TemplateException;

            @Override
            default void close() { };
        }
    }

    private class Context
    {
        private final Config params;
        private final JsEngine.Evaluator evaluator;

        public Context(Config params, JsEngine.Evaluator evaluator)
        {
            this.params = params;
            this.evaluator = evaluator;
        }

        private ObjectNode evalObjectRecursive(ObjectNode local)
            throws TemplateException
        {
            ObjectNode built = local.objectNode();
            for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(local.fields())) {
                JsonNode value = pair.getValue();
                JsonNode evaluated;
                if (NO_EVALUATE_PARAMETERS.contains(pair.getKey())) {
                    // don't evaluate _do and _else_do parameters
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
            String resultText = evaluator.evaluate(code, scopedParams, jsonMapper);
            if (resultText == null) {
                return jsonMapper.getNodeFactory().nullNode();
            }
            else {
                return jsonMapper.getNodeFactory().textNode(resultText);
            }
        }
    }

    private static class CheckingJsEvaluator
        implements JsEngine.Evaluator
    {
        private final JsEngine.Evaluator evaluator;
        private final JsEngine.Evaluator checker;

        CheckingJsEvaluator(JsEngine.Evaluator evaluator, JsEngine.Evaluator checker)
        {
            this.evaluator = evaluator;
            this.checker = checker;
        }

        @Override
        public String evaluate(String code, Config scopedParams, ObjectMapper jsonMapper)
            throws TemplateException
        {
            String resultText = evaluator.evaluate(code, scopedParams, jsonMapper);
            String checkText = checker.evaluate(code, scopedParams, jsonMapper);
            if (resultText == null && checkText != null ||
                    resultText != null && !resultText.equals(checkText)) {
                logger.error("Detected incompatibility between Nashorn and Graal. Code: {}", code);
            }
            return resultText;
        }
    }

    protected Config eval(Config config, Config params)
        throws TemplateException
    {
        ObjectNode object = config.convert(ObjectNode.class);

        Object built;
        switch (jsEngineType) {
        case NASHORN:
            try (JsEngine.Evaluator evaluator = nashorn.newEvaluator(params)) {
                built = new Context(params, evaluator).evalObjectRecursive(object);
            }
            break;

        case GRAAL:
            try (JsEngine.Evaluator evaluator = graal.newEvaluator(params)) {
                built = new Context(params, evaluator).evalObjectRecursive(object);
            }
            break;

        case NASHORN_GRAAL_CHECK:
            try (JsEngine.Evaluator evaluator = nashorn.newEvaluator(params);
                    JsEngine.Evaluator checker = graal.newEvaluator(params);
                    JsEngine.Evaluator checkingEvaluator = new CheckingJsEvaluator(evaluator, checker)) {
                built = new Context(params, checkingEvaluator).evalObjectRecursive(object);
            }
            break;

        default:
            throw new UnsupportedOperationException();
        }

        return config.getFactory().create(built);
    }

    @Override
    public String template(String content, Config params)
        throws TemplateException
    {
        String resultText;
        switch (jsEngineType) {
        case NASHORN:
            try (JsEngine.Evaluator evaluator = nashorn.newEvaluator(params)) {
                resultText = evaluator.evaluate(content, params, jsonMapper);
            }
            break;

        case GRAAL:
            try (JsEngine.Evaluator evaluator = graal.newEvaluator(params)) {
                resultText = evaluator.evaluate(content, params, jsonMapper);
            }
            break;

        case NASHORN_GRAAL_CHECK:
            try (JsEngine.Evaluator evaluator = nashorn.newEvaluator(params);
                    JsEngine.Evaluator checker = graal.newEvaluator(params);
                    JsEngine.Evaluator checkingEvaluator = new CheckingJsEvaluator(evaluator, checker)) {
                resultText = checkingEvaluator.evaluate(content, params, jsonMapper);
            }
            break;

        default:
            throw new UnsupportedOperationException();
        }

        if (resultText == null) {
            return "";
        }
        else {
            return resultText;
        }
    }
}
