package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;
import static io.digdag.standards.scheduler.DailySchedulerFactory.parseAt;
import static io.digdag.standards.scheduler.DailySchedulerFactory.parseFragment;

public class MonthlySchedulerFactory
        implements SchedulerFactory
{
    private final ScheduleConfigHelper configHelper;

    @Inject
    public MonthlySchedulerFactory(ScheduleConfigHelper configHelper)
    {
        this.configHelper = configHelper;
    }

    @Override
    public String getType()
    {
        return "monthly";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        String desc = config.getOptional("_command", String.class).or(() -> config.get("at", String.class));

        String[] fragments = desc.split(",", 2);
        if (fragments.length != 2) {
            throw new ConfigException("monthly>: scheduler requires day,hh:mm:ss format: " + desc);
        }

        int day;
        try {
            day = Integer.parseInt(fragments[0]);
        }
        catch (NumberFormatException ex) {
            throw new ConfigException("monthly>: scheduler requires day,hh:mm:ss format: " + desc);
        }

        long dailyDelay = parseAt("monthly>", fragments[1]);

        Optional<Instant> start = configHelper.getDateTimeStart(config, "start", timeZone);
        Optional<Instant> end = configHelper.getDateTimeEnd(config, "end", timeZone);
        configHelper.validateStartEnd(start, end);

        return new CronScheduler(
                "0 0 " + day + " * *",
                timeZone,
                dailyDelay,
                start,
                end
        );
    }
}
