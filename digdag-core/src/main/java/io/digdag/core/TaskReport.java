package io.digdag.core;

import java.util.List;
import java.util.Map;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigFactory;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskReport.class)
@JsonDeserialize(as = ImmutableTaskReport.class)
public abstract class TaskReport
{
    public abstract List<Config> getInputs();

    public abstract List<Config> getOutputs();

    public abstract Config getCarryParams();

    // TODO metrics

    // TODO startedAt

    // TODO executedOnHost

    public static ImmutableTaskReport.Builder builder()
    {
        return ImmutableTaskReport.builder();
    }

    public static TaskReport empty(ConfigFactory cf)
    {
        return builder()
            .carryParams(cf.empty())
            .build();
    }
}
