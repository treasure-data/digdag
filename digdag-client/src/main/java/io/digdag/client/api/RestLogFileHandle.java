package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.net.URL;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
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

