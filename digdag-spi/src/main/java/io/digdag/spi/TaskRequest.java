package io.digdag.spi;

import java.util.UUID;
import java.time.Instant;
import java.time.ZoneId;
import org.immutables.value.Value;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskRequest.class)
@JsonDeserialize(as = ImmutableTaskRequest.class)
public interface TaskRequest
{
    int getSiteId();

    int getProjectId();

    Optional<String> getProjectName();

    String getWorkflowName();

    Optional<Long> getWorkflowDefinitionId();

    Optional<String> getRevision();

    long getTaskId();

    long getAttemptId();

    long getSessionId();

    Optional<String> getRetryAttemptName();

    String getTaskName();

    String getLockId();

    ZoneId getTimeZone();

    UUID getSessionUuid();

    Instant getSessionTime();

    Instant getCreatedAt();

    boolean isCancelRequested();

    Config getLocalConfig();

    Config getConfig();

    Config getLastStateParams();

    @Value.Default
    default int getRetryCount()
    {
        return 0;
    }

    @Value.Default
    default Optional<Instant> getStartedAt()
    {
        return Optional.absent();
    }

    static ImmutableTaskRequest.Builder builder()
    {
        return ImmutableTaskRequest.builder();
    }

    static TaskRequest withLockId(TaskRequest source, String lockId)
    {
        return ImmutableTaskRequest.builder()
            .from(source)
            .lockId(lockId)
            .build();
    }
}
