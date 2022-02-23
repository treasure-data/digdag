package io.digdag.core.schedule;

import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSchedule.class)
@JsonDeserialize(as = ImmutableStoredSchedule.class)
public abstract class StoredSchedule
        extends Schedule
{
    public abstract int getId();

    public abstract int getProjectId();

    public abstract Instant getCreatedAt();

    public abstract Instant getUpdatedAt();

    public abstract Optional<Instant> getDisabledAt();

    public abstract Optional<Instant> getLastSessionTime();

    public abstract Optional<Instant> getStartDate();

    public abstract Optional<Instant> getEndDate();
}
