package io.digdag.client.api;

import java.util.Date;
import java.util.List;
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

    public abstract Long getParentId();

    public abstract Config getConfig();

    public abstract List<Long> getUpstreams();

    public abstract boolean isGroup();

    public abstract String getState();

    public abstract Config getCarryParams();

    public abstract Config getStateParams();

    public abstract Date getUpdatedAt();

    public abstract Date getRetryAt();

    // TODO in out Report

    public static ImmutableRestTask.Builder builder()
    {
        return ImmutableRestTask.builder();
    }
}
