package io.digdag.standards.scheduler;

import java.time.ZoneId;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class MinutesIntervalSchedulerFactory
        implements SchedulerFactory
{
    @Override
    public String getType()
    {
        return "minutes_interval";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        int interval = config.get("command", int.class);
        long delay = config.get("delay", long.class, 0L);
        return new CronScheduler("0/" + interval + " * * * *", timeZone, delay);
    }
}
