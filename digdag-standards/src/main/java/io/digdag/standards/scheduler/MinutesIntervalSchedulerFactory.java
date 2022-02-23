package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
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
    public Scheduler newScheduler(Config config, ZoneId timeZone, Optional<Instant> startDate, Optional<Instant> endDate)
    {
        int interval = config.get("_command", int.class);
        long delay = config.get("delay", long.class, 0L);
        return new CronScheduler("*/" + interval + " * * * *", timeZone, delay, startDate, endDate);
    }
}
