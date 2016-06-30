package io.digdag.client.api;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableRestDirectUploadHandle.class)
@JsonDeserialize(as = ImmutableRestDirectUploadHandle.class)
public interface RestDirectUploadHandle
{
    String getType();

    String getUrl();

    static ImmutableRestDirectUploadHandle.Builder builder()
    {
        return ImmutableRestDirectUploadHandle.builder();
    }
}
