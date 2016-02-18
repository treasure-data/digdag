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
public abstract class RestTask
{
    public abstract long getId();

    public abstract String getFullName();

    public abstract Optional<Long> getParentId();

    public abstract Config getConfig();

    public abstract List<Long> getUpstreams();

    public abstract boolean isGroup();

    public abstract String getState();

    public abstract Config getExportParams();

    public abstract Config getStoreParams();

    public abstract Config getStateParams();

    public abstract Instant getUpdatedAt();

    public abstract Optional<Instant> getRetryAt();

    // TODO in out Report

    public static ImmutableRestTask.Builder builder()
    {
        return ImmutableRestTask.builder();
    }
}
