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

    Config getExportParams();

    Config getStoreParams();

    Config getStateParams();

    Instant getUpdatedAt();

    Optional<Instant> getRetryAt();

    Optional<Instant> getStartedAt();

    // TODO in out Report

    static ImmutableRestTask.Builder builder()
    {
        return ImmutableRestTask.builder();
    }
}
