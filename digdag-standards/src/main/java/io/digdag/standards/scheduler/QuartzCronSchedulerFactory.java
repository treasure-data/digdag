package io.digdag.standards.scheduler;

import java.time.ZoneId;
import io.digdag.client.config.Config;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class QuartzCronSchedulerFactory
        implements SchedulerFactory
{
    @Override
    public String getType()
    {
        return "quartz_cron";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        return new QuartzCronScheduler(
                config.get("_command", String.class),
                timeZone,
                config.get("delay", long.class, 0L));
    }
}
