package io.digdag.standards.scheduler;

import java.util.Date;
import java.util.TimeZone;
import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Predictor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronScheduler
        extends BaseScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(CronScheduler.class);

    private final SchedulingPattern pattern;
    private final ZoneId timeZone;
    private final long delaySeconds;
    protected final Optional<Instant> start;
    protected final Optional<Instant> end;

    CronScheduler(String cronPattern, ZoneId timeZone, long delaySeconds, Optional<Instant> start, Optional<Instant> end)
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
        this.start = start;
        this.end = end;
    }

    @Override
    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    @Override
    public ScheduleTime getFirstScheduleTime(Instant currentTime)
    {
        // truncate to seconds
        Instant truncated = Instant.ofEpochSecond(currentTime.getEpochSecond());
        if (truncated.equals(currentTime)) {
            // in this particular case, minus 1 second to include this currentTime
            // because Predictor doesn't include this time at nextMatchingTime() method
            truncated = truncated.minusSeconds(1);
        }
        Instant lastTime = truncated.minusSeconds(delaySeconds);

        return nextScheduleTime(lastTime);
    }

    @Override
    public ScheduleTime nextScheduleTime(Instant lastScheduleTime)
    {
        Instant next = nextWithStartEnd(lastScheduleTime);

        if (isScheduleFinished(next)) {
            return ScheduleTime.of(next, next);
        }
        else {
            return ScheduleTime.of(next, next.plusSeconds(delaySeconds));
        }
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
        return ScheduleTime.of(before, before.plusSeconds(delaySeconds));
    }

    private Instant next(Instant time)
    {
        Predictor predictor = new Predictor(pattern, Date.from(time));
        predictor.setTimeZone(TimeZone.getTimeZone(timeZone));
        return Instant.ofEpochMilli(predictor.nextMatchingTime());
    }

    private Instant nextWithStartEnd(Instant lastScheduleTime)
    {
        Instant next = next(lastScheduleTime);
        Instant nextRun = next.plusSeconds(delaySeconds);
        if (end.isPresent() && (end.get().equals(nextRun) || end.get().isBefore(nextRun))) {
            logger.debug("next run time is after to end. next_run:{}, end:{}", nextRun, end.get());
            next = SCHEDULE_END;
        }
        else if (start.isPresent() && start.get().isAfter(nextRun)) {
            logger.debug("next run time is before the start. next_run:{}, end:{}", nextRun, start.get());
            // next run is earlier than start. recalculate from start
            next = next(start.get().minusSeconds(1)); // -1s is required because predictor doesn't include this time at nextMatchingTime() method
        }
        return next;
    }
}
