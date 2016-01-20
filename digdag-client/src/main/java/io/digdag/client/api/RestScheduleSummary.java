package io.digdag.client.api;

import java.util.Date;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestScheduleSummary.class)
@JsonDeserialize(as = ImmutableRestScheduleSummary.class)
public abstract class RestScheduleSummary
{
    public abstract long getId();

    public abstract long getNextRunTime();

    public abstract long getNextScheduleTime();

    public abstract Date getCreatedAt();

    public abstract Date getUpdatedAt();

    public static ImmutableRestScheduleSummary.Builder builder()
    {
        return ImmutableRestScheduleSummary.builder();
    }
}
