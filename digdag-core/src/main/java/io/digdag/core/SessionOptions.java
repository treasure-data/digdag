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
@JsonSerialize(as = ImmutableSessionOptions.class)
@JsonDeserialize(as = ImmutableSessionOptions.class)
public abstract class SessionOptions
{
    public abstract Map<String, TaskReport> getSkipTaskMap();

    public static ImmutableSessionOptions.Builder sessionOptionsBuilder()
    {
        return ImmutableSessionOptions.builder();
    }
}
