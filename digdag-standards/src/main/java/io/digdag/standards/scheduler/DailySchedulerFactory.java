package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class DailySchedulerFactory
        implements SchedulerFactory
{
    private final ScheduleConfigHelper configHelper;

    @Inject
    public DailySchedulerFactory(ScheduleConfigHelper configHelper)
    {
        this.configHelper = configHelper;
    }

    @Override
    public String getType()
    {
        return "daily";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        String at = config.getOptional("_command", String.class).or(() -> config.get("at", String.class));
        Optional<Instant> start = configHelper.getDateTimeStart(config, "start", timeZone);
        Optional<Instant> end = configHelper.getDateTimeEnd(config, "end", timeZone);
        configHelper.validateStartEnd(start, end);

        return new CronScheduler(
                "0 0 * * *",
                timeZone,
                parseAt("daily>", at),
                start,
                end
        );
    }

    static long parseAt(String kind, String at)
    {
        String[] fragments = at.split(":");
        if (fragments.length != 3) {
            throw new ConfigException(kind + " scheduler requires hh:mm:ss format: " + at);
        }
        int hour = parseFragment(kind, fragments[0], at);
        int min = parseFragment(kind, fragments[1], at);
        int sec = parseFragment(kind, fragments[2], at);
        return hour * 3600 + min * 60 + sec;
    }

    static int parseFragment(String kind, String s, String at)
    {
        try {
            return Integer.parseInt(s);
        }
        catch (NumberFormatException ex) {
            throw new ConfigException(kind + " scheduler requires hh:mm:ss format: " + at);
        }
    }
}
