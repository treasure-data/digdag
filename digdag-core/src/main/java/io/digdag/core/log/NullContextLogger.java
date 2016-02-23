package io.digdag.core.log;

public class NullContextLogger
    implements ContextLogger
{
    @Override
    public void log(LogLevel level, long timestamp, String message)
    { }

    @Override
    public void close()
    { }
}
