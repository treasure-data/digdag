package io.digdag.core.repository;

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
