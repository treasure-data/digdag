package io.digdag.core.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.core.agent.ConfigEvalEngine.JsEngine;
import io.digdag.spi.TemplateException;
import java.io.IOException;
import java.time.ZoneId;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import static io.digdag.core.agent.ConfigEvalEngine.LIBRARY_JS_CONTENTS;

public class GraalJsEngine
    implements JsEngine
{
    private final Engine engine;
    private final Source[] libraryJsSources;

    public GraalJsEngine()
    {
        this.engine = Engine.newBuilder()
            .allowExperimentalOptions(true)
            .option("js.nashorn-compat", "true")
            .build();
        try {
            this.libraryJsSources = new Source[LIBRARY_JS_CONTENTS.length];
            for (int i = 0; i < LIBRARY_JS_CONTENTS.length; i++) {
                libraryJsSources[i] = Source.newBuilder("js", LIBRARY_JS_CONTENTS[i][1], LIBRARY_JS_CONTENTS[i][0]).build();
            }
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public JsEngine.Evaluator newEvaluator(Config params)
    {
        Context.Builder contextBuilder = Context.newBuilder()
            .engine(engine)
            .allowAllAccess(false)
            .timeZone(getWorkflowZoneId(params));
        Context context = contextBuilder.build();
        try {
            for (Source lib : libraryJsSources) {
                context.eval(lib);
            }
            GraalEvaluator evaluator = new GraalEvaluator(context);
            context = null;
            return evaluator;
        }
        finally {
            if (context != null) {
                context.close();
            }
        }
    }

    private static ZoneId getWorkflowZoneId(Config params)
    {
        return ZoneId.of(params.get("timezone", String.class));
    }

    private static class GraalEvaluator
            implements JsEngine.Evaluator
    {
        private final Context context;

        GraalEvaluator(Context context)
        {
            this.context = context;
        }

        @Override
        public String evaluate(String code, Config scopedParams, ObjectMapper jsonMapper)
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
                Value result = context.getBindings("js").getMember("template").execute(code, paramsJson);
                return result.asString();
            }
            catch (PolyglotException ex) {
                String message;
                if (ex.getCause() != null) {
                    message = ex.getCause().getMessage();
                }
                else {
                    message = ex.getMessage();
                }
                throw new TemplateException("Failed to evaluate a variable " + code + " (" + message + ")");
            }
        }

        @Override
        public void close()
        {
            context.close();
        }
    }
}
