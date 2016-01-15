package io.digdag.core.repository;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableScheduleSource.class)
public abstract class ScheduleSourceImpl
        extends ScheduleSource
{ }
