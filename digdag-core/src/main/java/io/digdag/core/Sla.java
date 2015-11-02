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
@JsonSerialize(as = ImmutableSla.class)
@JsonDeserialize(as = ImmutableSla.class)
public abstract class Sla
{
    public abstract int getSiteId();

    public abstract int getRepositoryId();

    public abstract int getWorkflowId();

    public abstract Optional<Integer> getHour();

    public abstract Optional<Integer> getMinute();

    public abstract ConfigSource getTask();

    // TODO more config here

    public static ImmutableSla.Builder builder()
    {
        return ImmutableSla.builder();
    }

    @Value.Check
    protected void check()
    {
        // TODO
    }
}
