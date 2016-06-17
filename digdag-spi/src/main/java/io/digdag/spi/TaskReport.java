package io.digdag.spi;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskReport.class)
@JsonDeserialize(as = ImmutableTaskReport.class)
public interface TaskReport
{
    List<Config> getInputs();

    List<Config> getOutputs();

    // TODO metrics

    // TODO startedAt

    // TODO executedOnHost

    static ImmutableTaskReport.Builder builder()
    {
        return ImmutableTaskReport.builder();
    }

    static TaskReport empty()
    {
        return builder()
            .build();
    }
}
