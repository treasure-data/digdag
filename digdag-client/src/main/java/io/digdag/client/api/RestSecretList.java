package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonDeserialize(as = ImmutableRestSecretList.class)
public interface RestSecretList
{
    List<RestSecretMetadata> secrets();

    static Builder builder() {
        return ImmutableRestSecretList.builder();
    }

    interface Builder
    {
        Builder secrets(Iterable<? extends RestSecretMetadata> secrets);

        RestSecretList build();
    }
}
