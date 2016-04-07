package io.digdag.spi;

import java.time.Instant;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLogFileHandle.class)
@JsonDeserialize(as = ImmutableLogFileHandle.class)
public abstract class LogFileHandle
{
    public abstract String getFileName();

    public abstract long getFileSize();

    public abstract String getTaskName();

    public abstract Instant getFirstLogTime();

    public abstract String getAgentId();

    public abstract Optional<DirectDownloadHandle> getDirect();

    public static ImmutableLogFileHandle.Builder builder()
    {
        return ImmutableLogFileHandle.builder();
    }
}
