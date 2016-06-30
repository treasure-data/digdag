package io.digdag.client.api;

import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableNameOptionalId.class)
@JsonDeserialize(as = ImmutableNameOptionalId.class)
public interface NameOptionalId
{
    String getName();

    Optional<Long> getId();

    static NameOptionalId of(String name, Optional<Long> id)
    {
        return ImmutableNameOptionalId.builder()
            .name(name)
            .id(id)
            .build();
    }
}
