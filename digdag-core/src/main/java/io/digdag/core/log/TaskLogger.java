package io.digdag.core.log;

import java.io.Closeable;
import java.util.Arrays;

import static io.digdag.core.log.TaskLogger.Stream.LOG;
import static java.nio.charset.StandardCharsets.UTF_8;

public interface TaskLogger
        extends Closeable
{
    enum Stream
    {
        OUT,
        ERR,
        LOG,
    }

    // called when log is produced in this process through slf4j
    default void log(LogLevel level, long timestamp, String message)
    {
        ImmutableLogEntry entry = ImmutableLogEntry.builder()
                .timestamp(timestamp)
                .stream(LOG)
                .body(message.getBytes(UTF_8))
                .level(level.toInt())
                .build();

        write(entry);
    }

    // called when a subprocess writes message to its stdout/stderr
    default void log(Stream stream, byte[] data, int off, int len)
    {
        byte[] body = Arrays.copyOfRange(data, off, len);

        ImmutableLogEntry entry = ImmutableLogEntry.builder()
                .timestamp(System.currentTimeMillis())
                .stream(stream)
                .body(body)
                .build();

        write(entry);
    }

    void write(LogEntry entry);

    @Override
    void close();
}
