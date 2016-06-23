package io.digdag.spi;

import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLogFilePrefix.class)
@JsonDeserialize(as = ImmutableLogFilePrefix.class)
public interface LogFilePrefix
{
    int getSiteId();

    int getProjectId();

    String getWorkflowName();

    Instant getSessionTime();

    ZoneId getTimeZone();

    Optional<String> getRetryAttemptName();

    Instant getCreatedAt();

    static ImmutableLogFilePrefix.Builder builder()
    {
        return ImmutableLogFilePrefix.builder();
    }
}
