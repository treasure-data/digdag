package io.digdag.core.workflow;

import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.spi.config.Config;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflow.class)
@JsonDeserialize(as = ImmutableWorkflow.class)
public abstract class Workflow
{
    public abstract String getName();

    public abstract Config getMeta();

    public abstract WorkflowTaskList getTasks();

    public static ImmutableWorkflow.Builder workflowBuilder()
    {
        return ImmutableWorkflow.builder();
    }

    public static Workflow of(String name, Config meta, WorkflowTaskList tasks)
    {
        return workflowBuilder()
            .name(name)
            .meta(meta)
            .tasks(tasks)
            .build();
    }

    @Value.Check
    protected void check()
    {
        checkState(!getName().isEmpty(), "Name of a workflow must not be empty");
        checkState(!getTasks().isEmpty(), "A workflow must have at least one task");
    }
}
