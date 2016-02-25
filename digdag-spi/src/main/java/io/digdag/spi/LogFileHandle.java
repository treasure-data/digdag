package io.digdag.spi;

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

    public abstract Optional<DirectDownloadHandle> getDirect();

    public static LogFileHandle ofNonDirect(String fileName)
    {
        return ImmutableLogFileHandle.builder()
            .fileName(fileName)
            .direct(Optional.absent())
            .build();
    }

    public static LogFileHandle ofDirect(String fileName, DirectDownloadHandle direct)
    {
        return ImmutableLogFileHandle.builder()
            .fileName(fileName)
            .direct(direct)
            .build();
    }
}
