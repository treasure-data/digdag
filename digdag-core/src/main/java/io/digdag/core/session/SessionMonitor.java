package io.digdag.core.session;

import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;
import io.digdag.core.spi.config.Config;

@JsonDeserialize(as = ImmutableSessionMonitor.class)
public abstract class SessionMonitor
{
    public abstract Config getConfig();

    public abstract Date getNextRunTime();

    public static ImmutableSessionMonitor.Builder sessionMonitorBuilder()
    {
        return ImmutableSessionMonitor.builder();
    }

    public static SessionMonitor of(Config config, Date nextRunTime)
    {
        return sessionMonitorBuilder()
            .config(config)
            .nextRunTime(nextRunTime)
            .build();
    }
}
