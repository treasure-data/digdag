package io.digdag.standards.scheduler;

import java.util.Date;
import java.util.TimeZone;

import io.digdag.core.spi.ScheduleTime;
import io.digdag.core.spi.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Predictor;

public abstract class AbstractCronScheduler
        implements Scheduler
{
    private final SchedulingPattern pattern;
    private final TimeZone timeZone;
    private final long delaySeconds;

    AbstractCronScheduler(String cronPattern, TimeZone timeZone, long delaySeconds)
    {
        this.pattern = new SchedulingPattern(cronPattern);
        this.timeZone = timeZone;
        this.delaySeconds = delaySeconds;
    }

    @Override
    public TimeZone getTimeZone()
    {
        return timeZone;
    }

    @Override
    public ScheduleTime getFirstScheduleTime(Date currentTime)
    {
        Date startTime = currentTime;  // TODO make this from config
        // align to the scheduling time. cron4j uses per-minute scheduling
        return nextScheduleTime(new Date(startTime.getTime() - 60*1000));
    }

    @Override
    public ScheduleTime nextScheduleTime(Date lastScheduleTime)
    {
        Predictor predictor = new Predictor(pattern, lastScheduleTime);
        predictor.setTimeZone(timeZone);
        long msec = predictor.nextMatchingTime();
        return ScheduleTime.of(
                new Date(msec + delaySeconds*1000),
                new Date(msec));
    }
}
