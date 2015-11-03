package io.digdag.standards;

import java.util.TimeZone;
import io.digdag.core.config.Config;
import io.digdag.core.spi.Scheduler;
import io.digdag.core.spi.SchedulerFactory;

public class CronSchedulerFactory
        implements SchedulerFactory
{
    @Override
    public boolean matches(Config config)
    {
        return config.has("cron");
    }

    @Override
    public Scheduler newScheduler(Config config)
    {
        return new CronScheduler(
                config.get("cron", String.class),
                TimeZone.getTimeZone(config.get("timezone", String.class, "UTC")),
                config.get("delay", Integer.class, 0));
    }

    public static class CronScheduler
            extends AbstractCronScheduler
    {
        public CronScheduler(String cronPattern, TimeZone timeZone, long delaySeconds)
        {
            super(cronPattern, timeZone, delaySeconds);
        }
    }
}
