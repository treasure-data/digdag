package io.digdag.core;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableWorkflowSource.class)
public abstract class WorkflowSource
{
    public abstract String getName();

    public abstract ConfigSource getConfig();

    public static ImmutableWorkflowSource.Builder workflowSourceBuilder()
    {
        return ImmutableWorkflowSource.builder();
    }

    public static WorkflowSource of(String name, ConfigSource config)
    {
        return workflowSourceBuilder()
            .name(name)
            .config(config)
            .build();
    }
}
