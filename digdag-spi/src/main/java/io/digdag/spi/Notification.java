package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;

@Value.Immutable
@JsonSerialize(as = ImmutableNotification.class)
@JsonDeserialize(as = ImmutableNotification.class)
public abstract class Notification
{
    // A message and timestamp
    @JsonProperty("timestamp") public abstract Instant getTimestamp();
    @JsonProperty("message") public abstract String getMessage();

    // Optional context information with further details about the origin of the notification
    @JsonProperty("site_id") public abstract Optional<Integer> getSiteId();
    @JsonProperty("project_id") public abstract Optional<Integer> getProjectId();
    @JsonProperty("project_name") public abstract Optional<String> getProjectName();
    @JsonProperty("workflow_name") public abstract Optional<String> getWorkflowName();
    @JsonProperty("revision") public abstract Optional<String> getRevision();
    @JsonProperty("attempt_id") public abstract Optional<Long> getAttemptId();
    @JsonProperty("session_id") public abstract Optional<Long> getSessionId();
    @JsonProperty("task_name") public abstract Optional<String> getTaskName();
    @JsonProperty("timezone") public abstract Optional<ZoneId> getTimeZone();
    @JsonProperty("session_uuid") public abstract Optional<UUID> getSessionUuid();
    @JsonProperty("session_time") public abstract Optional<OffsetDateTime> getSessionTime();

    public static ImmutableNotification.Builder builder(Instant timestamp, String message) {
        return ImmutableNotification.builder()
                .timestamp(timestamp)
                .message(message);
    }
}
