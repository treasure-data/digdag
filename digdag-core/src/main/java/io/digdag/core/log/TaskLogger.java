package io.digdag.core.log;

import java.io.Closeable;

public interface TaskLogger
    extends Closeable
{
    // called when log is produced in this process through slf4j
    void log(LogLevel level, long timestamp, String message);

    // called when a subprocess writes message to its stdout/stderr
    void log(byte[] data, int off, int len);

    @Override
    void close();
}
