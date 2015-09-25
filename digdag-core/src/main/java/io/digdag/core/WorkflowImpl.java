package io.digdag.core;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableWorkflow.class)
public abstract class WorkflowImpl
        extends Workflow
{ }
