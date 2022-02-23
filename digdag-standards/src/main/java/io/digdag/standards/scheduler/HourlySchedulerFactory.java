package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class HourlySchedulerFactory
        implements SchedulerFactory
{
    @Override
    public String getType()
    {
        return "hourly";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone, Optional<Instant> startDate, Optional<Instant> endDate)
    {
        String at = config.getOptional("_command", String.class).or(() -> config.get("at", String.class));
        return new CronScheduler("0 * * * *", timeZone, parseAt(at), startDate, endDate);
    }

    private long parseAt(String at)
    {
        String[] fragments = at.split(":");
        if (fragments.length != 2) {
            throw new ConfigException("hourly>: scheduler requires mm:ss format: " + at);
        }
        int min = parseFragment(fragments[0], at);
        int sec = parseFragment(fragments[1], at);
        return min * 60 + sec;
    }

    private int parseFragment(String s, String at)
    {
        try {
            return Integer.parseInt(s);
        }
        catch (NumberFormatException ex) {
            throw new ConfigException("hourly>: scheduler requires mm:ss format: " + at);
        }
    }
}
