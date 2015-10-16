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
@JsonSerialize(as = ImmutableAction.class)
@JsonDeserialize(as = ImmutableAction.class)
public abstract class Action
{
    public abstract int getSiteId();

    public abstract long getTaskId();

    public abstract String getFullName();

    public abstract ConfigSource getConfig();

    public abstract ConfigSource getParams();

    public abstract ConfigSource getStateParams();

    public static ImmutableAction.Builder actionBuilder()
    {
        return ImmutableAction.builder();
    }

    @Value.Check
    protected void check()
    {
        // TODO
    }
}
