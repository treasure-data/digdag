package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestScheduleEnableByModeRequest.class)
public interface RestScheduleEnableByModeRequest
{
    Optional<String> getMode();
    Optional<String> getLocalTime();

    static ImmutableRestScheduleEnableByModeRequest.Builder builder()
    {
        return ImmutableRestScheduleEnableByModeRequest.builder();
    }
}
