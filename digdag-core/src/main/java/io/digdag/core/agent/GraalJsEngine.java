package io.digdag.core.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.core.agent.ConfigEvalEngine.JsEngine;
import io.digdag.spi.TemplateException;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.function.Supplier;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.core.agent.ConfigEvalEngine.LIBRARY_JS_CONTENTS;

public class GraalJsEngine
    implements JsEngine
{
    private final Source[] libraryJsSources;
    private final boolean extendedSyntax;
    private final Engine sharedEngine;

    private static final HostAccess hostAccess = HostAccess.newBuilder()
            .allowPublicAccess(true)
            .build();

    public GraalJsEngine(boolean extendedSyntax)
    {
        this.extendedSyntax = extendedSyntax;
        try {
            this.libraryJsSources = new Source[LIBRARY_JS_CONTENTS.length];
            for (int i = 0; i < LIBRARY_JS_CONTENTS.length; i++) {
                libraryJsSources[i] = Source.newBuilder("js", LIBRARY_JS_CONTENTS[i][1], LIBRARY_JS_CONTENTS[i][0]).build();
            }
            this.sharedEngine = createEngine();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public JsEngine.Evaluator newEvaluator(Config params)
    {
        GraalEvaluatorWithRetry evaluator = new GraalEvaluatorWithRetry(params, sharedEngine, extendedSyntax, libraryJsSources);
        return evaluator;
    }

    private static Engine createEngine()
    {
        return Engine.newBuilder()
                .allowExperimentalOptions(true)
                .option("js.nashorn-compat", "true")
                .option("js.ecmascript-version", "5")
                .option("js.syntax-extensions", "false")
                .option("js.console", "false")
                .option("js.load", "true")           //Same as default. Should be false?
                .option("js.load-from-url", "false") //Same as default
                .build();
    }

    private static Supplier<Context> createContextSupplier(Optional<Engine> sharedEngine, Config params, Source[] libraryJsSources)
    {
        Engine engine = sharedEngine.isPresent() ? sharedEngine.get(): createEngine();
        return () -> {
            Context.Builder contextBuilder = Context.newBuilder()
                    .engine(engine)
                    .allowAllAccess(false)
                    .allowHostAccess(hostAccess) // Need interoperability for String methods (e.g. replaceAll)
                    .allowHostClassLookup(className -> {
                        if (className.matches("java\\.lang\\.String")) { // Restrict to java.lang.String
                            return true;
                        }
                        else {
                            return false;
                        }
                    })
                    .timeZone(getWorkflowZoneId(params));
            Context context = contextBuilder.build();
            try {
                for (Source lib : libraryJsSources) {
                    context.eval(lib);
                }
            }
            catch (RuntimeException ex) {
                context.close();
                throw ex;
            }
            return context;
        };
    }

    private static ZoneId getWorkflowZoneId(Config params)
    {
        return ZoneId.of(params.get("timezone", String.class));
    }

    private static class GraalEvaluatorWithRetry
            implements JsEngine.Evaluator
    {
        private static Logger logger = LoggerFactory.getLogger(GraalEvaluatorWithRetry.class);

        private final Config params;
        private final Engine sharedEngine;
        private final boolean extendedSyntax;
        private final Source[] libraryJsSources;

        public GraalEvaluatorWithRetry(Config params, Engine sharedEngine, boolean extendedSyntax, Source[] libraryJsSources)
        {
            this.params = params;
            this.sharedEngine = sharedEngine;
            this.extendedSyntax = extendedSyntax;
            this.libraryJsSources = libraryJsSources;
        }

        @Override
        public String evaluate(String code, Config scopedParams, ObjectMapper jsonMapper)
                throws TemplateException
        {
            try {
                return new GraalEvaluator(createContextSupplier(Optional.of(sharedEngine), params, libraryJsSources)
                        , extendedSyntax).evaluate(code, scopedParams, jsonMapper);
            }
            catch (IllegalStateException e) {
                /**
                 *  When shutdown sequence started, engine is closed before call and tasks will fail.
                 *  To avoid it, create new engine and retry.
                 */
                if (e.getMessage().equals("Engine is already closed.")) {
                    logger.debug("Engine is already closed. Retry with new engine");
                    return new GraalEvaluator(createContextSupplier(Optional.empty(), params, libraryJsSources)
                            , extendedSyntax).evaluate(code, scopedParams, jsonMapper);
                }
                else {
                    throw e;
                }
            }
        }
    }

    private static class GraalEvaluator
            implements JsEngine.Evaluator
    {
        private final Supplier<Context> contextSupplier;
        private final boolean extendedSyntax;

        GraalEvaluator(Supplier<Context> contextSupplier, boolean extendedSyntax)
        {
            this.contextSupplier = contextSupplier;
            this.extendedSyntax = extendedSyntax;
        }

        @Override
        public String evaluate(String code, Config scopedParams, ObjectMapper jsonMapper)
                throws TemplateException
        {
            try(Context context = contextSupplier.get()) {
                String paramsJson;
                try {
                    paramsJson = jsonMapper.writeValueAsString(scopedParams);
                }
                catch (RuntimeException | IOException ex) {
                    throw new TemplateException("Failed to serialize parameters to JSON", ex);
                }
                try {
                    Value result = context.getBindings("js").getMember("template").execute(code, paramsJson, extendedSyntax);
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
        }
    }
}
