package io.digdag.core.spi;

import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.core.config.Config;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskResult.class)
@JsonDeserialize(as = ImmutableTaskResult.class)
public abstract class TaskResult
{
    public abstract Config getSubtaskConfig();

    public abstract TaskReport getReport();

    public static ImmutableTaskResult.Builder builder()
    {
        return ImmutableTaskResult.builder();
    }
}
