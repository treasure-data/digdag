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
@JsonSerialize(as = ImmutableSlaSchedule.class)
@JsonDeserialize(as = ImmutableSlaSchedule.class)
public abstract class SlaSchedule
{
    public abstract Sla getSla();

    public abstract ConfigSource getScheduler();

    public static ImmutableSlaSchedule.Builder builder()
    {
        return ImmutableSlaSchedule.builder();
    }
}
