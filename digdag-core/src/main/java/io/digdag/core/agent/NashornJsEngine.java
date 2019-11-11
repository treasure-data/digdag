package io.digdag.core.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.core.agent.ConfigEvalEngine.JsEngine;
import io.digdag.spi.TemplateException;
import java.io.IOException;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import static io.digdag.core.agent.ConfigEvalEngine.LIBRARY_JS_CONTENTS;

public class NashornJsEngine
    implements JsEngine
{
    private final NashornScriptEngineFactory jsEngineFactory;

    public NashornJsEngine()
    {
        this.jsEngineFactory = new NashornScriptEngineFactory();
    }

    public JsEngine.Evaluator newEvaluator(Config params)
    {
        ScriptEngine scriptEngine = jsEngineFactory.getScriptEngine(new String[] {
            //"--language=es6",  // this is not even accepted with jdk1.8.0_20 and has a bug with jdk1.8.0_51
            "--no-java",
            "--no-syntax-extensions",
            "-timezone=" + params.get("timezone", String.class),
        });
        try {
            for (int i = 0; i < LIBRARY_JS_CONTENTS.length; i++) {
                scriptEngine.eval(LIBRARY_JS_CONTENTS[i][1]);
            }
        }
        catch (ScriptException | ClassCastException ex) {
            throw new IllegalStateException("Unexpected script evaluation failure", ex);
        }
        final Invocable invocable = (Invocable) scriptEngine;
        return (code, scopedParams, jsonMapper) -> invokeTemplate(invocable, code, scopedParams, jsonMapper);
    }

    private String invokeTemplate(Invocable templateInvocable, String code, Config scopedParams, ObjectMapper jsonMapper)
        throws TemplateException
    {
        String paramsJson;
        try {
            paramsJson = jsonMapper.writeValueAsString(scopedParams);
        }
        catch (RuntimeException | IOException ex) {
            throw new TemplateException("Failed to serialize parameters to JSON", ex);
        }
        try {
            return (String) templateInvocable.invokeFunction("template", code, paramsJson);
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
}
