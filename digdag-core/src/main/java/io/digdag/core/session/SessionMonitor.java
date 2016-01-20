package io.digdag.core.session;

import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableSessionMonitor.class)
public abstract class SessionMonitor
{
    public abstract String getType();

    public abstract Config getConfig();

    public abstract Date getNextRunTime();

    public static ImmutableSessionMonitor.Builder sessionMonitorBuilder()
    {
        return ImmutableSessionMonitor.builder();
    }

    public static SessionMonitor of(String type, Config config, Date nextRunTime)
    {
        return sessionMonitorBuilder()
            .type(type)
            .config(config)
            .nextRunTime(nextRunTime)
            .build();
    }
}
