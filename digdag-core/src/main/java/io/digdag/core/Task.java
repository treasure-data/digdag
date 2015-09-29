package io.digdag.core;

import java.util.List;
import java.util.Map;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableStoredSession.class)
public abstract class Task
{
    public abstract int getSessionId();

    public abstract Optional<Long> getParentId();

    public abstract String getName();

    public abstract TaskFlags getFlags();

    public abstract WorkflowTaskOptions getOptions();

    public abstract ConfigSource getConfig();

    public abstract List<Long> getUpstreams();  // list of task_id
}
