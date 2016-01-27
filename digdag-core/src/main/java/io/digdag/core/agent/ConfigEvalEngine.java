package io.digdag.core.agent;

import java.util.Map;
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
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public class ConfigEvalEngine
{
    private static final String evalCodeScript =
        "function evalCode(code, js) {" +
            "return JSON.stringify((new Function(\"with(this) { \" + code + \" }\")).call(JSON.parse(js)))" +
        "}";

    private final ObjectMapper jsonMapper;
    private final ScriptEngine scriptEngine;
    private final Invocable invocable;

    @Inject
    public ConfigEvalEngine()
    {
        this.jsonMapper = new ObjectMapper();
        this.scriptEngine = new ScriptEngineManager().getEngineByExtension("js");
        try {
            scriptEngine.eval(evalCodeScript);
            invocable = (Invocable) scriptEngine;
        }
        catch (ScriptException | ClassCastException ex) {
            throw new IllegalStateException("Unexpected script evaluation failure", ex);
        }
    }

    public Config eval(Path archivePath, Config config)
        throws ConfigEvalException
    {
        ObjectNode object = config.convert(ObjectNode.class);
        evalRecursive(object, object);
        return config.getFactory().wrap(object);
    }

    private void evalRecursive(ObjectNode object, ObjectNode root)
        throws ConfigEvalException
    {
        for (Map.Entry<String, JsonNode> pair : ImmutableList.copyOf(object.fields())) {  // copy to prevent concurrent modification
            String key = pair.getKey();
            JsonNode value = pair.getValue();
            if (key.endsWith("=")) {
                String name = key.substring(0, key.length() - 1);
                String code;
                if (value.isTextual()) {
                    code = value.textValue();
                }
                else {
                    code = value.toString();
                }
                object.set(name, evalValue(code, root));
            }
            else {
                if (value.isObject()) {
                    evalRecursive((ObjectNode) value, root);
                }
                else if (value.isArray()) {
                    evalRecursiveArray((ArrayNode) value, root);
                }
            }
        }
    }

    private void evalRecursiveArray(ArrayNode array, ObjectNode root)
        throws ConfigEvalException
    {
        for (JsonNode element : array) {
            if (element.isObject()) {
                evalRecursive((ObjectNode) element, root);
            }
            else if (element.isArray()) {
                evalRecursiveArray((ArrayNode) element, root);
            }
        }
    }

    private JsonNode evalValue(String code, ObjectNode params)
        throws ConfigEvalException
    {
        try {
            String js = jsonMapper.writeValueAsString(params);
            String result = (String) invocable.invokeFunction("evalCode", code, js);
            if (result == null) {
                return jsonMapper.getNodeFactory().nullNode();
            }
            else {
                return jsonMapper.readTree(result);
            }
        }
        catch (ScriptException | NoSuchMethodException | IOException ex) {
            throw new ConfigEvalException("Failed to evaluate JavaScript code: " + code, ex);
        }
    }
}
