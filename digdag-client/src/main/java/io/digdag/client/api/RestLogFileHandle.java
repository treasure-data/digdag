package io.digdag.client.api;

import java.time.Instant;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableRestLogFileHandle.class)
@JsonDeserialize(as = ImmutableRestLogFileHandle.class)
public abstract class RestLogFileHandle
{
    public abstract String getFileName();

    public abstract String getTaskName();

    public abstract Instant getFileTime();

    public abstract String getAgentId();

    public abstract Optional<RestDirectDownloadHandle> getDirect();

    public static ImmutableRestLogFileHandle.Builder builder()
    {
        return ImmutableRestLogFileHandle.builder();
    }
}

