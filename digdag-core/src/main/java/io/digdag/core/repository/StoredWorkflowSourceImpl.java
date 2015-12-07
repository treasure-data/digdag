package io.digdag.core.repository;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableStoredWorkflowSource.class)
public abstract class StoredWorkflowSourceImpl
        extends StoredWorkflowSource
{ }
