package io.digdag.core.workflow;

import java.util.List;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.session.TaskType;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ModelValidator;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflowTask.class)
@JsonDeserialize(as = ImmutableWorkflowTask.class)
public abstract class WorkflowTask
{
    public abstract String getName();

    public abstract String getFullName();

    public abstract int getIndex();

    public abstract Optional<Integer> getParentIndex();

    public abstract List<Integer> getUpstreamIndexes();

    public abstract TaskType getTaskType();

    public abstract Config getConfig();

    public static class Builder extends ImmutableWorkflowTask.Builder { }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkRawTaskName("name", getName())
            .check("task index", getIndex(), getIndex() >= 0, "must not be negative")
            .validate("workflow task", this);
    }
}
