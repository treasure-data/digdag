package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonDeserialize(as = ImmutableRestSecretMetadata.class)
public interface RestSecretMetadata
{
    String key();

    static RestSecretMetadata of(String key)
    {
        return builder().key(key).build();
    }

    static Builder builder()
    {
        return ImmutableRestSecretMetadata.builder();
    }

    interface Builder
    {
        Builder key(String key);

        RestSecretMetadata build();
    }
}
