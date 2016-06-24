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

    static DirectDownloadHandle of(String type, String url)
    {
        return ImmutableDirectDownloadHandle.builder()
            .type(type)
            .url(url)
            .build();
    }
}
