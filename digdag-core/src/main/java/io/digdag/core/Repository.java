package io.digdag.core;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableRepository.class)
public abstract class Repository
{
    public abstract String getName();

    //public abstract Config getConfig();

    //public abstract boolean isDisabled();

    public static ImmutableRepository.Builder repositoryBuilder()
    {
        return ImmutableRepository.builder();
    }

    public static Repository of(String name)
    {
        return repositoryBuilder()
            .name(name)
            .build();
    }
}
