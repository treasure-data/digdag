package io.digdag.spi;

import java.time.ZoneId;
import io.digdag.client.config.Config;

public interface SchedulerFactory
{
    boolean matches(Config config);

    Scheduler newScheduler(Config config, ZoneId timeZone);
}
