package io.digdag.core.workflow;

import java.util.List;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.session.TaskType;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.core.spi.config.Config;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflowTask.class)
@JsonDeserialize(as = ImmutableWorkflowTask.class)
public abstract class WorkflowTask
{
    public abstract String getName();

    public abstract int getIndex();

    public abstract Optional<Integer> getParentIndex();

    public abstract List<Integer> getUpstreamIndexes();

    public abstract TaskType getTaskType();

    public abstract Config getConfig();

    public static class Builder extends ImmutableWorkflowTask.Builder { }

    @Value.Check
    protected void check()
    {
        checkState(!getName().isEmpty(), "name of a task must not be empty");
        checkState(getIndex() >= 0, "index of a task must not be negative");
    }
}
