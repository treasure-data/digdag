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
public abstract class RestWorkflowSessionTime
{
    public abstract IdName getProject();

    public abstract String getRevision();

    public abstract OffsetDateTime getSessionTime();

    @JsonProperty("timezone")
    public abstract ZoneId getTimeZone();

    public static ImmutableRestWorkflowSessionTime.Builder builder()
    {
        return ImmutableRestWorkflowSessionTime.builder();
    }
}
