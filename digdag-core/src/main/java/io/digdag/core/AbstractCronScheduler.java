package io.digdag.core;

import java.util.Date;
import java.util.TimeZone;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Predictor;

public abstract class AbstractCronScheduler
        implements Scheduler
{
    private final SchedulingPattern pattern;
    private final TimeZone timeZone;

    AbstractCronScheduler(String cronPattern, TimeZone timeZone)
    {
        this.pattern = new SchedulingPattern(cronPattern);
        this.timeZone = timeZone;
    }

    @Override
    public Date getFirstScheduleTime(Date currentTime)
    {
        Date startTime = currentTime;  // TODO make this from config
        // align to the scheduling time. cron4j uses per-minute scheduling
        return nextScheduleTime(new Date(startTime.getTime() - 60*1000));
    }

    @Override
    public Date nextScheduleTime(Date lastScheduleTime)
    {
        Predictor predictor = new Predictor(pattern, lastScheduleTime);
        predictor.setTimeZone(timeZone);
        return new Date(predictor.nextMatchingTime());
    }
}
