package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;

@Value.Immutable
@JsonSerialize(as = ImmutableNotification.class)
@JsonDeserialize(as = ImmutableNotification.class)
public interface Notification
{
    // A message and timestamp
    @JsonProperty("timestamp")
    Instant getTimestamp();

    @JsonProperty("message")
    String getMessage();

    // Optional context information with further details about the origin of the notification
    @JsonProperty("site_id")
    Optional<Integer> getSiteId();

    @JsonProperty("project_id")
    Optional<Integer> getProjectId();

    @JsonProperty("project_name")
    Optional<String> getProjectName();

    @JsonProperty("workflow_name")
    Optional<String> getWorkflowName();

    @JsonProperty("revision")
    Optional<String> getRevision();

    @JsonProperty("attempt_id")
    Optional<Long> getAttemptId();

    @JsonProperty("session_id")
    Optional<Long> getSessionId();

    @JsonProperty("task_name")
    Optional<String> getTaskName();

    @JsonProperty("timezone")
    Optional<ZoneId> getTimeZone();

    @JsonProperty("session_uuid")
    Optional<UUID> getSessionUuid();

    @JsonProperty("session_time")
    Optional<OffsetDateTime> getSessionTime();

    @JsonProperty("workflow_definition_id")
    Optional<Long> getWorkflowDefinitionId();

    static ImmutableNotification.Builder builder(Instant timestamp, String message) {
        return ImmutableNotification.builder()
                .timestamp(timestamp)
                .message(message);
    }
}
