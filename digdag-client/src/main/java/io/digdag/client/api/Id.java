package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonCreator;

public interface Id
{
    String get();

    @JsonCreator
    static Id of(String id)
    {
        return new ImmutableId(id);
    }
}
