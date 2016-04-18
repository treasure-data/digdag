package io.digdag.core.repository;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(as = ImmutableStoredWorkflowDefinition.class)
public abstract class StoredWorkflowDefinition
        extends WorkflowDefinition
{
    public abstract long getId();

    public abstract int getRevisionId();
}
