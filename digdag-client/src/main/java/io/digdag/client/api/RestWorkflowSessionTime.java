package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestWorkflowSessionTime.class)
public interface RestWorkflowSessionTime
{
    IdAndName getProject();

    String getRevision();

    OffsetDateTime getSessionTime();

    @JsonProperty("timezone")
    ZoneId getTimeZone();

    static ImmutableRestWorkflowSessionTime.Builder builder()
    {
        return ImmutableRestWorkflowSessionTime.builder();
    }
}
