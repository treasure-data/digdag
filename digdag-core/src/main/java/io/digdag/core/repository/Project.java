package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import java.util.regex.Pattern;

@JsonDeserialize(as = ImmutableProject.class)
public abstract class Project
{
    public abstract String getName();

    public static Project of(String name)
    {
        return ImmutableProject.builder()
            .name(name)
            .build();
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkProjectName("name", getName())
            .validate("project", this);
    }
}
