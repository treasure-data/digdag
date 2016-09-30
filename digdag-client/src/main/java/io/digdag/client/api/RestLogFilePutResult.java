package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestLogFilePutResult.class)
public interface RestLogFilePutResult
{
    String getFileName();

    static RestLogFilePutResult of(String fileName)
    {
        return ImmutableRestLogFilePutResult.builder()
            .fileName(fileName)
            .build();
    }
}

