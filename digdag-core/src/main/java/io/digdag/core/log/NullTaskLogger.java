package io.digdag.core.log;

public class NullTaskLogger
    implements TaskLogger
{
    @Override
    public void log(LogLevel level, long timestamp, String message)
    { }

    @Override
    public void log(Stream stream, byte[] data, int off, int len)
    { }

    @Override
    public void close()
    { }
}
