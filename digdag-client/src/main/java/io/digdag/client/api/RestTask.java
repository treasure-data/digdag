package io.digdag.client.api;

import java.time.Instant;
import java.util.List;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestTask.class)
@JsonDeserialize(as = ImmutableRestTask.class)
public interface RestTask
{
    long getId();

    String getFullName();

    Optional<Long> getParentId();

    Config getConfig();

    List<Long> getUpstreams();

    boolean isGroup();

    String getState();

    Config getExportParams();

    Config getStoreParams();

    Config getStateParams();

    Instant getUpdatedAt();

    Optional<Instant> getRetryAt();

    // TODO in out Report

    static ImmutableRestTask.Builder builder()
    {
        return ImmutableRestTask.builder();
    }
}
