package io.digdag.core;

import java.util.Date;
import java.util.TimeZone;

public class CronScheduler
        extends AbstractCronScheduler
{
    public CronScheduler(ConfigSource config)
    {
        super(config.get("cron", String.class),
                TimeZone.getTimeZone(config.get("timezone", String.class, "UTC")));
    }
}
