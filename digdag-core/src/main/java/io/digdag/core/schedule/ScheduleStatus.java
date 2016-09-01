package io.digdag.core.schedule;

import java.time.Instant;
import org.immutables.value.Value;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.spi.ScheduleTime;

@Value.Immutable
@JsonDeserialize(as = ImmutableScheduleStatus.class)
public interface ScheduleStatus
{
    ScheduleTime getNextScheduleTime();

    Optional<Instant> getLastScheduleTime();

    static ScheduleStatus of(ScheduleTime nextScheduleTime, Optional<Instant> lastScheduleTime)
    {
        return ImmutableScheduleStatus.builder()
            .nextScheduleTime(nextScheduleTime)
            .lastScheduleTime(lastScheduleTime)
            .build();
    }
}
