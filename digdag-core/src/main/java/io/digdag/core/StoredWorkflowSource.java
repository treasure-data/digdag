package io.digdag.core;

import java.util.List;
import java.util.Map;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableStoredWorkflowSource.class)
public abstract class StoredWorkflowSource
        extends WorkflowSource
{
    public abstract int getId();

    public abstract int getRevisionId();
}
