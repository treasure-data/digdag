package io.digdag.core.workflow;

import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ModelValidator;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflow.class)
@JsonDeserialize(as = ImmutableWorkflow.class)
public abstract class Workflow
{
    public abstract String getName();

    public abstract Config getMeta();

    public abstract WorkflowTaskList getTasks();

    public static ImmutableWorkflow.Builder builder()
    {
        return ImmutableWorkflow.builder();
    }

    public static Workflow of(String name, Config meta, WorkflowTaskList tasks)
    {
        return ImmutableWorkflow.builder()
            .name(name)
            .meta(meta)
            .tasks(tasks)
            .build();
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkTaskName("name", getName())
            .check("tasks", getTasks(), !getTasks().isEmpty(), "must not be empty")
            .validate("workflow", this);
    }
}
