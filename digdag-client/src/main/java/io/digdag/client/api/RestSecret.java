package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSecret.class)
public interface RestSecret
{
    String key();

    IdAndName project();

    static RestSecret of(String key)
    {
        return builder().key(key).build();
    }

    static Builder builder()
    {
        return ImmutableRestSecret.builder();
    }

    interface Builder
    {
        Builder key(String key);
        Builder project(IdAndName project);
        RestSecret build();
    }

}
