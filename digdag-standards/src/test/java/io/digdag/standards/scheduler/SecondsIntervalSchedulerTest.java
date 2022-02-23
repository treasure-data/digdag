package io.digdag.standards.scheduler;

import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import io.digdag.standards.scheduler.SecondsIntervalSchedulerFactory.SecondsIntervalScheduler;
import org.junit.Test;

import java.time.Instant;

import static java.time.ZoneOffset.UTC;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SecondsIntervalSchedulerTest
{
    @Test
    public void testGetFirstScheduleTime()
            throws Exception
    {
        SecondsIntervalScheduler scheduler = new SecondsIntervalScheduler(UTC, 3, 2, Optional.absent(), Optional.absent());

        assertThat(scheduler.getFirstScheduleTime(Instant.ofEpochSecond(3)),
                is(ScheduleTime.of(Instant.ofEpochSecond(3), Instant.ofEpochSecond(5), Optional.absent(), Optional.absent())));

        assertThat(scheduler.getFirstScheduleTime(Instant.ofEpochSecond(4)),
                is(ScheduleTime.of(Instant.ofEpochSecond(6), Instant.ofEpochSecond(8), Optional.absent(), Optional.absent())));
    }

    @Test
    public void testNextScheduleTime()
            throws Exception
    {
        SecondsIntervalScheduler scheduler = new SecondsIntervalScheduler(UTC, 3, 2, Optional.absent(), Optional.absent());

        assertThat(scheduler.nextScheduleTime(Instant.ofEpochSecond(3)),
                is(ScheduleTime.of(Instant.ofEpochSecond(6), Instant.ofEpochSecond(8), Optional.absent(), Optional.absent())));

        assertThat(scheduler.nextScheduleTime(Instant.ofEpochSecond(4)),
                is(ScheduleTime.of(Instant.ofEpochSecond(6), Instant.ofEpochSecond(8), Optional.absent(), Optional.absent())));
    }

    @Test
    public void testLastScheduleTime()
            throws Exception
    {
        SecondsIntervalScheduler scheduler = new SecondsIntervalScheduler(UTC, 3, 2, Optional.absent(), Optional.absent());

        assertThat(scheduler.lastScheduleTime(Instant.ofEpochSecond(3)),
                is(ScheduleTime.of(Instant.ofEpochSecond(0), Instant.ofEpochSecond(2), Optional.absent(), Optional.absent())));

        assertThat(scheduler.lastScheduleTime(Instant.ofEpochSecond(4)),
                is(ScheduleTime.of(Instant.ofEpochSecond(3), Instant.ofEpochSecond(5), Optional.absent(), Optional.absent())));
    }
}