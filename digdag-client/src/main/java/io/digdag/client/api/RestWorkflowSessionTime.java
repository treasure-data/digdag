package io.digdag.client.api;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestWorkflowSessionTime.class)
@JsonDeserialize(as = ImmutableRestWorkflowSessionTime.class)
public interface RestWorkflowSessionTime
{
    IdName getProject();

    String getRevision();

    OffsetDateTime getSessionTime();

    @JsonProperty("timezone")
    ZoneId getTimeZone();

    static ImmutableRestWorkflowSessionTime.Builder builder()
    {
        return ImmutableRestWorkflowSessionTime.builder();
    }
}
