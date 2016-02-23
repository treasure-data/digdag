package io.digdag.core.log;

public class ContextLogging
{
    private static final InheritableThreadLocal<Context> contexts = new InheritableThreadLocal<Context>();

    public static void enter(String level, ContextLogger logger)
    {
        enter(LogLevel.of(level), logger);
    }

    public static void enter(LogLevel level, ContextLogger logger)
    {
        if (getContext() != null) {
            // should here push logger to a stack rather than exception?
            throw new IllegalStateException("Context logger already set");
        }
        contexts.set(new Context(level, logger));
    }

    public static void leave()
    {
        Context ctx = getContext();
        if (ctx != null) {
            ctx.getLogger().close();
            contexts.set(null);
        }
    }

    public static Context getContext()
    {
        return contexts.get();
    }

    public static class Context
    {
        private final int filterLevel;
        private final ContextLogger logger;

        private Context(LogLevel filterLevel, ContextLogger logger)
        {
            this.filterLevel = filterLevel.toInt();
            this.logger = logger;
        }

        public boolean matches(LogLevel level)
        {
            return filterLevel <= level.toInt();
        }

        public ContextLogger getLogger()
        {
            return logger;
        }
    }
}
