package io.digdag.core.session;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.core.workflow.TaskConfig;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableTask.class)
public abstract class Task
{
    public abstract long getSessionId();

    public abstract Optional<Long> getParentId();

    public abstract String getFullName();

    public abstract TaskConfig getConfig();

    public abstract TaskType getTaskType();

    public abstract TaskStateCode getState();

    public static ImmutableTask.Builder taskBuilder()
    {
        return ImmutableTask.builder();
    }
}
