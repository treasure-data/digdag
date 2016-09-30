package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableNameOptionalId.class)
public interface NameOptionalId
{
    String getName();

    Optional<Id> getId();

    static NameOptionalId of(String name, Optional<Id> id)
    {
        return ImmutableNameOptionalId.builder()
            .name(name)
            .id(id)
            .build();
    }
}
