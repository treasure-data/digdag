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

    public abstract ConfigSource getOptions();

    //public abstract boolean isDisabled();

    public static ImmutableRepository.Builder repositoryBuilder()
    {
        return ImmutableRepository.repositoryBuilder();
    }

    public static Repository of(String name, ConfigSource options)
    {
        return repositoryBuilder()
            .name(name)
            .options(options)
            .build();
    }
}
