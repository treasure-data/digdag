package io.digdag.client.api;

import com.google.common.base.Preconditions;
import com.fasterxml.jackson.annotation.JsonValue;

class ImmutableId implements Id
{
    private final String id;

    ImmutableId(String id)
    {
        this.id = Preconditions.checkNotNull(id, "id");
    }

    @Override
    @JsonValue
    public String get()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        return another instanceof ImmutableId
            && id.equals(((ImmutableId) another).get());
    }
}
