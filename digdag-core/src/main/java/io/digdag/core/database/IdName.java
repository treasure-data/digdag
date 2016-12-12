package io.digdag.core.database;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableIdName.class)
interface IdName
{
    int getId();

    String getName();

    static IdName of(int id, String name)
    {
        return ImmutableIdName.builder()
            .id(id)
            .name(name)
            .build();
    }
}
