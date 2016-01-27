package io.digdag.standards.scheduler;

import java.util.Date;
import java.util.TimeZone;
import java.time.Instant;
import java.time.ZoneId;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Predictor;

public abstract class AbstractCronScheduler
        implements Scheduler
{
    private final SchedulingPattern pattern;
    private final ZoneId timeZone;
    private final long delaySeconds;

    AbstractCronScheduler(String cronPattern, ZoneId timeZone, long delaySeconds)
    {
        this.pattern = new SchedulingPattern(cronPattern);
        this.timeZone = timeZone;
        this.delaySeconds = delaySeconds;
    }

    @Override
    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    @Override
    public ScheduleTime getFirstScheduleTime(Instant currentTime)
    {
        Instant startTime = currentTime;  // TODO make this from config
        return nextScheduleTime(startTime);
    }

    @Override
    public ScheduleTime nextScheduleTime(Instant lastScheduleTime)
    {
        Predictor predictor = new Predictor(pattern, Date.from(lastScheduleTime));
        predictor.setTimeZone(TimeZone.getTimeZone(timeZone));
        long msec = predictor.nextMatchingTime();
        return ScheduleTime.of(
                Instant.ofEpochSecond(msec / 1000 + delaySeconds),
                Instant.ofEpochSecond(msec / 1000));
    }
}
