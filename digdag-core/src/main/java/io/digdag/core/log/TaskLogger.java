package io.digdag.core.log;

import java.io.Closeable;

public interface TaskLogger
    extends Closeable
{
    void log(LogLevel level, long timestamp, String message);

    @Override
    void close();
}
