package io.digdag.core;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableWorkflowSource.class)
public abstract class WorkflowSourceImpl
        extends WorkflowSource
{ }
