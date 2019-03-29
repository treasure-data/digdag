package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestTask.class)
public interface RestTask
{
    Id getId();

    String getFullName();

    Optional<Id> getParentId();

    Config getConfig();

    List<Id> getUpstreams();

    boolean isGroup();

    String getState();

    // Note: Only reason why here sets a default value is for backward compatibility.
    // Because older versions don't contain cancelRequested field, default value is
    // necessary to avoid "cancelRequsted field doesn't exist" errors.
    @Value.Default
    default boolean getCancelRequested()
    {
        return false;
    }

    Config getExportParams();

    Config getStoreParams();

    Config getStateParams();

    Instant getUpdatedAt();

    Optional<Instant> getRetryAt();

    Optional<Instant> getStartedAt();

    Config getError();

    // TODO in out Report

    static ImmutableRestTask.Builder builder()
    {
        return ImmutableRestTask.builder();
    }
}
