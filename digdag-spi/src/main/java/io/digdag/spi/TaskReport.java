package io.digdag.spi;

import java.util.List;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskReport.class)
@JsonDeserialize(as = ImmutableTaskReport.class)
public abstract class TaskReport
{
    public abstract List<Config> getInputs();

    public abstract List<Config> getOutputs();

    // TODO metrics

    // TODO startedAt

    // TODO executedOnHost

    public static ImmutableTaskReport.Builder builder()
    {
        return ImmutableTaskReport.builder();
    }

    public static TaskReport empty()
    {
        return builder()
            .build();
    }
}
