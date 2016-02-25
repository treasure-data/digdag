package io.digdag.client.api;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableRestDirectUploadHandle.class)
@JsonDeserialize(as = ImmutableRestDirectUploadHandle.class)
public abstract class RestDirectUploadHandle
{
    public abstract String getType();

    public abstract String getUrl();

    public static ImmutableRestDirectUploadHandle.Builder builder()
    {
        return ImmutableRestDirectUploadHandle.builder();
    }
}
