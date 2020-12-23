package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestScheduleEnableRequest.class)
public interface RestScheduleEnableRequest
{
    Optional<Boolean> getSkipSchedule();
    Optional<String> getNextTime();

    static ImmutableRestScheduleEnableRequest.Builder builder()
    {
        return ImmutableRestScheduleEnableRequest.builder();
    }
}
