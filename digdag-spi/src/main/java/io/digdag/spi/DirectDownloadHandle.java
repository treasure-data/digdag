package io.digdag.spi;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableDirectDownloadHandle.class)
@JsonDeserialize(as = ImmutableDirectDownloadHandle.class)
public abstract class DirectDownloadHandle
{
    public abstract String getType();

    public abstract String getUrl();

    public static ImmutableDirectDownloadHandle.Builder builder()
    {
        return ImmutableDirectDownloadHandle.builder();
    }
}
