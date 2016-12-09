package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestScheduleCollection.class)
public interface RestScheduleCollection
{
    List<RestSchedule> getSchedules();

    static ImmutableRestScheduleCollection.Builder builder()
    {
        return ImmutableRestScheduleCollection.builder();
    }
}
