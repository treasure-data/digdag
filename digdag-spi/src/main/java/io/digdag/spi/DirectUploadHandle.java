package io.digdag.spi;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableDirectUploadHandle.class)
@JsonDeserialize(as = ImmutableDirectUploadHandle.class)
public abstract class DirectUploadHandle
{
    public abstract String getType();

    public abstract String getUrl();

    public static ImmutableDirectUploadHandle.Builder builder()
    {
        return ImmutableDirectUploadHandle.builder();
    }
}
