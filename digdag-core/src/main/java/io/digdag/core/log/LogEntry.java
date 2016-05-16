package io.digdag.core.log;

import org.immutables.value.Value;

@Value.Immutable
public abstract class LogEntry
{
    abstract long timestamp();
    abstract TaskLogger.Stream stream();
    abstract int level();
    abstract byte[] body();
}
