package io.digdag.standards.scheduler;

import java.util.Date;
import java.util.TimeZone;
import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Predictor;

public class CronScheduler
        implements Scheduler
{
    private final SchedulingPattern pattern;
    private final ZoneId timeZone;
    private final long delaySeconds;
    private final Optional<Instant> startDate;
    private final Optional<Instant> endDate;

    CronScheduler(String cronPattern, ZoneId timeZone, long delaySeconds, Optional<Instant> startDate, Optional<Instant> endDate)
    {
        this.pattern = new SchedulingPattern(cronPattern) {
            // workaround for a bug of cron4j:
            // https://gist.github.com/frsyuki/618c4e6c1f5f876e4ee74b9da2fd37c0
            @Override
            public boolean match(long millis)
            {
                return match(TimeZone.getTimeZone(timeZone), millis);
            }
        };
        this.timeZone = timeZone;
        this.delaySeconds = delaySeconds;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @Override
    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    @Override
    public Optional<Instant> getStartDate()
    {
        return startDate;
    }

    @Override
    public Optional<Instant> getEndDate()
    {
        return endDate;
    }

    @Override
    public ScheduleTime getFirstScheduleTime(Instant currentTime)
    {
        Instant startTime = currentTime;  // TODO make this from config

        // truncate to seconds
        Instant truncated = Instant.ofEpochSecond(currentTime.getEpochSecond());
        if (truncated.equals(currentTime)) {
            // in this particular case, minus 1 second to include this currentTime
            // because Predictor doesn't include this time at "next"MatchingTime() method
            truncated = truncated.minusSeconds(1);
        }
        Instant lastTime = truncated.minusSeconds(delaySeconds);

        return nextScheduleTime(lastTime);
    }

    @Override
    public ScheduleTime nextScheduleTime(Instant lastScheduleTime)
    {
        Instant next = next(lastScheduleTime);
        return ScheduleTime.of(next, next.plusSeconds(delaySeconds), startDate, endDate);
    }

    @Override
    public ScheduleTime lastScheduleTime(Instant currentScheduleTime)
    {
        // estimate interval (doesn't have to be exact value)
        Instant next = next(currentScheduleTime);
        Instant nextNext = next(next);
        long estimatedInterval = nextNext.getEpochSecond() - next.getEpochSecond();

        // get an aligned time that is before currentScheduleTime
        Instant before = currentScheduleTime.minusSeconds(estimatedInterval);
        do {
            before = before.minusSeconds(estimatedInterval);
        } while(!before.isBefore(currentScheduleTime));

        // before is before currentScheduleTime but not sure how many times before. calculate it.
        Instant nextOfBefore = next(before);
        while (nextOfBefore.isBefore(currentScheduleTime)) {
            before = nextOfBefore;
            nextOfBefore = next(before);
        }

        // nextOfBefore is same with currentScheduleTime or after currentScheduleTime. nextOfBefore is next of before. done.
        return ScheduleTime.of(before, before.plusSeconds(delaySeconds), startDate, endDate);
    }

    private Instant next(Instant time)
    {
        Predictor predictor = new Predictor(pattern, Date.from(time));
        predictor.setTimeZone(TimeZone.getTimeZone(timeZone));
        return Instant.ofEpochMilli(predictor.nextMatchingTime());
    }
}
