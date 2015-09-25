package io.digdag.core;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableTask.class)
public abstract class TaskImpl
        extends Task
{ }
