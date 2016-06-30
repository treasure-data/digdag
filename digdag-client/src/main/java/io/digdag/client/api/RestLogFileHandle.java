package io.digdag.client.api;

import java.time.Instant;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableRestLogFileHandle.class)
@JsonDeserialize(as = ImmutableRestLogFileHandle.class)
public interface RestLogFileHandle
{
    String getFileName();

    long getFileSize();

    String getTaskName();

    Instant getFileTime();

    String getAgentId();

    Optional<RestDirectDownloadHandle> getDirect();

    static ImmutableRestLogFileHandle.Builder builder()
    {
        return ImmutableRestLogFileHandle.builder();
    }
}

