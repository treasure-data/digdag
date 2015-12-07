package io.digdag.core.schedule;

import io.digdag.core.repository.ImmutableImplStyle;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@ImmutableImplStyle
@JsonSerialize(as = ImmutableSchedule.class)
public abstract class ScheduleImpl
        extends Schedule
{ }
