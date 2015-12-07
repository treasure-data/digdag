package io.digdag.core.session;

import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.spi.TaskReport;
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

    public static ImmutableSessionOptions.Builder builder()
    {
        return ImmutableSessionOptions.builder();
    }

    public static SessionOptions empty()
    {
        return builder().build();
    }
}
