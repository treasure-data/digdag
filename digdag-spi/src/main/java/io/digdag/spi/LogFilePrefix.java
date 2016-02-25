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
public abstract class LogFilePrefix
{
    public abstract int getSiteId();

    public abstract String getRepositoryName();

    public abstract String getWorkflowName();

    public abstract Instant getSessionTime();

    public abstract ZoneId getTimeZone();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Instant getCreatedAt();

    public static ImmutableLogFilePrefix.Builder builder()
    {
        return ImmutableLogFilePrefix.builder();
    }
}
