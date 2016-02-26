package io.digdag.client.api;

import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableRestLogFilePutResult.class)
@JsonDeserialize(as = ImmutableRestLogFilePutResult.class)
public abstract class RestLogFilePutResult
{
    public abstract String getFileName();

    public static RestLogFilePutResult of(String fileName)
    {
        return ImmutableRestLogFilePutResult.builder()
            .fileName(fileName)
            .build();
    }
}

