package io.digdag.core.queue;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.core.config.Config;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableAction.class)
@JsonDeserialize(as = ImmutableAction.class)
public abstract class Action
{
    public abstract int getSiteId();

    public abstract long getTaskId();

    public abstract String getFullName();

    public abstract Config getConfig();

    public abstract Config getParams();

    public abstract Config getStateParams();

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
