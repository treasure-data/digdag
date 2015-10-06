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

@Value.Immutable
@JsonSerialize(as = ImmutableTaskReport.class)
@JsonDeserialize(as = ImmutableTaskReport.class)
public abstract class TaskReport
{
    @JsonProperty("in")
    public abstract List<ConfigSource> getInputs();

    @JsonProperty("out")
    public abstract List<ConfigSource> getOutputs();

    public abstract ConfigSource getCarryParams();

    // TODO metrics

    // TODO startedAt

    // TODO executedOnHost

    public static ImmutableTaskReport.Builder reportBuilder()
    {
        return ImmutableTaskReport.builder();
    }

    public static TaskReport empty(ConfigSourceFactory cf)
    {
        return reportBuilder()
            .carryParams(cf.create())
            .build();
    }
}
