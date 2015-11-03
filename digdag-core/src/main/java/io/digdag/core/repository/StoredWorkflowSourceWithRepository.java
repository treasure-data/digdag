package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredWorkflowSourceWithRepository.class)
@JsonDeserialize(as = ImmutableStoredWorkflowSourceWithRepository.class)
public abstract class StoredWorkflowSourceWithRepository
        extends StoredWorkflowSource
{
    public abstract StoredRepository getRepository();

    // TODO include RevisionSummary
}
