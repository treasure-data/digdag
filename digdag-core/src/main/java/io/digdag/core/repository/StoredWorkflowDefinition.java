package io.digdag.core.repository;

import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(as = ImmutableStoredWorkflowDefinition.class)
public abstract class StoredWorkflowDefinition
        extends WorkflowDefinition
{
    public abstract long getId();

    public abstract int getRevisionId();

    // timeZone is denormalized when storing a workflow definition.
    // to get this from a non-stored workflow, use WorkflowDefinition.getTimeZoneOfWorkflow.
    public abstract ZoneId getTimeZone();
}
