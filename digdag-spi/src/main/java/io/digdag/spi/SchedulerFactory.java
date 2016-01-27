package io.digdag.spi;

import java.time.ZoneId;
import io.digdag.client.config.Config;

public interface SchedulerFactory
{
    String getType();

    Scheduler newScheduler(Config config, ZoneId timeZone);
}
