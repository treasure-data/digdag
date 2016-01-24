package io.digdag.core.repository;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableWorkflowDefinition.class)
public abstract class WorkflowDefinitionImpl
        extends WorkflowDefinition
{ }
