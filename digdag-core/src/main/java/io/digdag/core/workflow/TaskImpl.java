package io.digdag.core.workflow;

import io.digdag.core.config.ImmutableImplStyle;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableTask.class)
public abstract class TaskImpl
        extends Task
{ }
