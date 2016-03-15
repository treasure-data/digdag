package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import java.util.regex.Pattern;

@JsonDeserialize(as = ImmutableRepository.class)
public abstract class Repository
{
    public abstract String getName();

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

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkResourceName("name", getName())
            .validate("repository", this);
    }
}
