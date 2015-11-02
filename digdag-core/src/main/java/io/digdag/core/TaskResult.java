package io.digdag.core;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskResult.class)
@JsonDeserialize(as = ImmutableTaskResult.class)
public abstract class TaskResult
{
    public abstract ConfigSource getSubtaskConfig();

    public abstract TaskReport getReport();

    public static ImmutableTaskResult.Builder builder()
    {
        return ImmutableTaskResult.builder();
    }
}
