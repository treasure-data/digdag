package io.digdag.spi;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableDirectDownloadHandle.class)
@JsonDeserialize(as = ImmutableDirectDownloadHandle.class)
public interface DirectDownloadHandle
{
    String getType();

    String getUrl();

    static ImmutableDirectDownloadHandle.Builder builder()
    {
        return ImmutableDirectDownloadHandle.builder();
    }
}
