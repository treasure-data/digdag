package io.digdag.core.repository;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableStoredWorkflowDefinition.class)
public abstract class StoredWorkflowDefinitionImpl
        extends StoredWorkflowDefinition
{ }
