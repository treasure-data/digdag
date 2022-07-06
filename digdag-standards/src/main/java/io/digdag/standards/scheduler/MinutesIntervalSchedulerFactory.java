package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class MinutesIntervalSchedulerFactory
        implements SchedulerFactory
{
    private final ScheduleConfigHelper configHelper;

    @Inject
    public MinutesIntervalSchedulerFactory(ScheduleConfigHelper configHelper)
    {
        this.configHelper = configHelper;
    }

    @Override
    public String getType()
    {
        return "minutes_interval";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        int interval = config.get("_command", int.class);
        long delay = config.get("delay", long.class, 0L);
        Optional<Instant> start = configHelper.getDateTimeStart(config, "start", timeZone);
        Optional<Instant> end = configHelper.getDateTimeEnd(config, "end", timeZone);
        configHelper.validateStartEnd(start, end);

        return new CronScheduler(
                "*/" + interval + " * * * *",
                timeZone,
                delay,
                start,
                end
        );
    }
}
