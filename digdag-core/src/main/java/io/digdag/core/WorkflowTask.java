package io.digdag.core;

import java.util.List;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflowTask.class)
@JsonDeserialize(as = ImmutableWorkflowTask.class)
public abstract class WorkflowTask
{
    public abstract String getName();

    public abstract int getTaskIndex();

    public abstract Optional<Integer> getParentTaskIndex();

    public abstract List<Integer> getUpstreamTaskIndexes();

    public abstract WorkflowTaskOption getOption();

    public abstract ConfigSource getConfig();

    public static class Builder extends ImmutableWorkflowTask.Builder { }

    @Value.Check
    protected void check()
    {
        checkState(!getName().isEmpty(), "name of a task must not be empty");
        checkState(getTaskIndex() >= 0, "taskIndex of a task must not be negative");
    }
}
