package io.digdag.spi;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableDirectUploadHandle.class)
@JsonDeserialize(as = ImmutableDirectUploadHandle.class)
public interface DirectUploadHandle
{
    String getType();

    String getUrl();

    static DirectUploadHandle of(String type, String url)
    {
        return ImmutableDirectUploadHandle.builder()
            .type(type)
            .url(url)
            .build();
    }
}
