package io.digdag.core;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflow.class)
@JsonDeserialize(as = ImmutableWorkflow.class)
public abstract class Workflow
{
    public abstract String getName();

    public abstract ConfigSource getMeta();

    public abstract List<WorkflowTask> getTasks();

    public static ImmutableWorkflow.Builder workflowBuilder()
    {
        return ImmutableWorkflow.builder();
    }

    public static Workflow of(String name, ConfigSource meta, List<WorkflowTask> tasks)
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
