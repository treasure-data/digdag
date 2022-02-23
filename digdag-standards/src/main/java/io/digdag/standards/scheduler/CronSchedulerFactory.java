package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class CronSchedulerFactory
        implements SchedulerFactory
{
    @Override
    public String getType()
    {
        return "cron";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone, Optional<Instant> startDate, Optional<Instant> endDate)
    {
        return new CronScheduler(
                config.get("_command", String.class),
                timeZone,
                config.get("delay", long.class, 0L),
                startDate,
                endDate);
    }
}
