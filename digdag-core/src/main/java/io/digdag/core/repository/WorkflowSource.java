package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.config.Config;

@JsonDeserialize(as = ImmutableWorkflowSource.class)
public abstract class WorkflowSource
{
    public abstract String getName();

    public abstract Config getConfig();

    public static ImmutableWorkflowSource.Builder workflowSourceBuilder()
    {
        return ImmutableWorkflowSource.builder();
    }

    public static WorkflowSource of(String name, Config config)
    {
        return workflowSourceBuilder()
            .name(name)
            .config(config)
            .build();
    }
}
