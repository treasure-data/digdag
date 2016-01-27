package io.digdag.standards.scheduler;

import java.time.ZoneId;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

public class DailySchedulerFactory
        implements SchedulerFactory
{
    @Override
    public String getType()
    {
        return "daily";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        String at = config.getOptional("command", String.class).or(() -> config.get("at", String.class));
        return new CronScheduler("0 0 * * *", timeZone, parseAt(at));
    }

    private long parseAt(String at)
    {
        String[] fragments = at.split(":");
        if (fragments.length != 3) {
            throw new ConfigException("daily>: scheduler requires hh:mm:ss format: " + at);
        }
        int hour = parseFragment(fragments[0], at);
        int min = parseFragment(fragments[1], at);
        int sec = parseFragment(fragments[2], at);
        return hour * 3600 + min * 60 + sec;
    }

    private int parseFragment(String s, String at)
    {
        try {
            return Integer.parseInt(s);
        }
        catch (NumberFormatException ex) {
            throw new ConfigException("daily>: scheduler requires hh:mm:ss format: " + at);
        }
    }
}
