package io.digdag.client.api;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableRestDirectDownloadHandle.class)
@JsonDeserialize(as = ImmutableRestDirectDownloadHandle.class)
public abstract class RestDirectDownloadHandle
{
    public abstract String getType();

    public abstract String getUrl();

    public static ImmutableRestDirectDownloadHandle.Builder builder()
    {
        return ImmutableRestDirectDownloadHandle.builder();
    }
}
