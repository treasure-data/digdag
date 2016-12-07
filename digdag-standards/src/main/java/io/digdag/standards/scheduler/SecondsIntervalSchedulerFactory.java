package io.digdag.standards.scheduler;

import com.google.common.base.Preconditions;
import io.digdag.client.config.Config;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.spi.SchedulerFactory;

import java.time.Instant;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkArgument;

public class SecondsIntervalSchedulerFactory
        implements SchedulerFactory
{
    @Override
    public String getType()
    {
        return "seconds_interval";
    }

    @Override
    public Scheduler newScheduler(Config config, ZoneId timeZone)
    {
        int interval = config.get("_command", int.class);
        long delay = config.get("delay", long.class, 0L);
        return new SecondsIntervalScheduler(timeZone, interval, delay);
    }

    static class SecondsIntervalScheduler
            implements Scheduler
    {
        private final ZoneId timeZone;
        private final int interval;
        private final long delay;

        SecondsIntervalScheduler(ZoneId timeZone, int interval, long delay)
        {
            this.timeZone = timeZone;
            this.interval = interval;
            this.delay = delay;
        }

        @Override
        public ZoneId getTimeZone()
        {
            return timeZone;
        }

        @Override
        public ScheduleTime getFirstScheduleTime(Instant currentTime)
        {
            checkArgument(currentTime.getEpochSecond() > 0);
            return nextScheduleTime(currentTime.minusNanos(1));
        }

        @Override
        public ScheduleTime nextScheduleTime(Instant lastScheduleTime)
        {
            checkArgument(lastScheduleTime.getEpochSecond() < Instant.MAX.getEpochSecond() - interval);
            long truncatedEpoch = lastScheduleTime.getEpochSecond() - (lastScheduleTime.getEpochSecond() % interval);
            long nextEpoch = truncatedEpoch + interval;
            Instant next = Instant.ofEpochSecond(nextEpoch);
            return ScheduleTime.of(next, next.plusSeconds(delay));
        }

        @Override
        public ScheduleTime lastScheduleTime(Instant currentScheduleTime)
        {
            checkArgument(currentScheduleTime.getEpochSecond() > 0);
            currentScheduleTime = currentScheduleTime.minusNanos(1);
            long lastEpoch = currentScheduleTime.getEpochSecond() - (currentScheduleTime.getEpochSecond() % interval);
            Instant last = Instant.ofEpochSecond(lastEpoch);
            return ScheduleTime.of(last, last.plusSeconds(delay));
        }
    }
}
