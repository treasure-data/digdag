package io.digdag.core;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@JsonDeserialize(as = ImmutableSessionMonitor.class)
public abstract class SessionMonitor
{
    public abstract ConfigSource getConfig();

    public abstract Date getNextRunTime();

    public static ImmutableSessionMonitor.Builder sessionMonitorBuilder()
    {
        return ImmutableSessionMonitor.builder();
    }

    public static SessionMonitor of(ConfigSource config, Date nextRunTime)
    {
        return sessionMonitorBuilder()
            .config(config)
            .nextRunTime(nextRunTime)
            .build();
    }
}
