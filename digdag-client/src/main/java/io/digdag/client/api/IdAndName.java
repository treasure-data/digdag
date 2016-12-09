package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableIdAndName.class)
public interface IdAndName
{
    Id getId();

    String getName();

    static IdAndName of(Id id, String name)
    {
        return ImmutableIdAndName.builder()
            .id(id)
            .name(name)
            .build();
    }
}
