package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableWorkflowDefinition.class)
public abstract class WorkflowDefinition
{
    public abstract String getName();

    public abstract Config getConfig();

    public static ImmutableWorkflowDefinition.Builder workflowSourceBuilder()
    {
        return ImmutableWorkflowDefinition.builder();
    }

    public static WorkflowDefinition of(String name, Config config)
    {
        return workflowSourceBuilder()
            .name(name)
            .config(config)
            .build();
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkTaskName("name", getName())
            .validate("workflow", this);
    }
}
