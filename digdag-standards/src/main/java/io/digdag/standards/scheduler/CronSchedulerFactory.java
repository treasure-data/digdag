package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class CronSchedulerFactory
        implements SchedulerFactory
{
    private final ScheduleConfigHelper configHelper;

    @Inject
    public CronSchedulerFactory(ScheduleConfigHelper configHelper)
    {
        this.configHelper = configHelper;
    }

    @Override
    public String getType()
    {
        return "cron";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        Optional<Instant> start = configHelper.getDateTimeStart(config, "start", timeZone);
        Optional<Instant> end = configHelper.getDateTimeEnd(config, "end", timeZone);
        configHelper.validateStartEnd(start, end);

        return new CronScheduler(
                config.get("_command", String.class),
                timeZone,
                config.get("delay", long.class, 0L),
                start,
                end
                );
    }
}
