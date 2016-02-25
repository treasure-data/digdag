package io.digdag.client.api;

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

    public abstract Optional<RestDirectDownloadHandle> getDirect();

    public static ImmutableRestLogFileHandle.Builder builder()
    {
        return ImmutableRestLogFileHandle.builder();
    }
}

