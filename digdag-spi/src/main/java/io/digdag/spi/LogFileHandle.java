package io.digdag.spi;

import java.time.Instant;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLogFileHandle.class)
@JsonDeserialize(as = ImmutableLogFileHandle.class)
public interface LogFileHandle
{
    String getFileName();

    long getFileSize();

    String getTaskName();

    Instant getFirstLogTime();

    String getAgentId();

    Optional<DirectDownloadHandle> getDirect();

    static ImmutableLogFileHandle.Builder builder()
    {
        return ImmutableLogFileHandle.builder();
    }
}
