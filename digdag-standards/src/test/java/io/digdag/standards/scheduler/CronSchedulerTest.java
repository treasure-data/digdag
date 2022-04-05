package io.digdag.standards.scheduler;

import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CronSchedulerTest extends SchedulerTestHelper
{
    Scheduler newScheduler(String pattern, String timeZone)
    {
        return new CronSchedulerFactory().newScheduler(newConfig(pattern), ZoneId.of(timeZone));
    }

    @Test
    public void firstScheduleTimeUtc()
    {
        // schedule is 10:00:00 every day

        // current time is 09:59:59
        {
            Instant currentTime = instant("2016-02-03 09:59:59 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 10:00:00 +0000"),
                            instant("2016-02-03 10:00:00 +0000"))));
        }

        // current time is 10:00:00
        {
            Instant currentTime = instant("2016-02-03 10:00:00 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 10:00:00 +0000"),
                            instant("2016-02-03 10:00:00 +0000"))));
        }

        // current time is 10:00:01
        {
            Instant currentTime = instant("2016-02-03 10:00:01 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-04 10:00:00 +0000"),
                            instant("2016-02-04 10:00:00 +0000"))));
        }
    }

    @Test
    public void firstScheduleTimeTz()
    {
        // same with firstScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant currentTime = instant("2016-02-03 09:59:59 +0900");
            assertThat(
                    newScheduler("00 10 * * *", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 10:00:00 +0900"),
                            instant("2016-02-03 10:00:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-03 10:00:00 +0900");
            assertThat(
                    newScheduler("00 10 * * *", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 10:00:00 +0900"),
                            instant("2016-02-03 10:00:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-03 10:00:01 +0900");
            assertThat(
                    newScheduler("00 10 * * *", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-04 10:00:00 +0900"),
                            instant("2016-02-04 10:00:00 +0900"))));
        }
    }

    @Test
    public void firstScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)

        // Current is at before DST, first will be at before DST
        {
            Instant currentTime = instant("2016-03-01 09:12:34 -0800");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 10:00:00 -0800"),
                            instant("2016-03-01 10:00:00 -0800"))));
        }
        // Current is at DST, first will be at DST
        {
            Instant currentTime = instant("2016-03-14 16:01:50 -0700");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-15 10:00:00 -0700"),
                            instant("2016-03-15 10:00:00 -0700"))));
        }

        // Current is at before DST, first will be at DST
        {
            Instant currentTime = instant("2016-03-13 01:59:59 -0800");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 10:00:00 -0700"),
                            instant("2016-03-13 10:00:00 -0700"))));
        }
    }

    @Test
    public void firstScheduleTimeMisc()
    {
        //Test for the currentTime with boundary value 00:00:00
        {
            Instant currentTime1 = instant("2016-03-13 00:00:00 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").getFirstScheduleTime(currentTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-13 10:00:00 +0000"),
                            instant("2016-03-13 10:00:00 +0000"))));
        }
        //Test for the currentTime with boundary value 10:00:00
        {
            Instant currentTime2 = instant("2016-03-13 10:00:00 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").getFirstScheduleTime(currentTime2),
                    is(ScheduleTime.of(
                            instant("2016-03-13 10:00:00 +0000"),
                            instant("2016-03-13 10:00:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeUtc()
    {
        // last schedule time is 00:00:00
        // schedule is 10:00:00 every day
        {
            Instant lastScheduleTime = instant("2016-02-03 00:00:00 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 10:00:00 +0000"),
                            instant("2016-02-03 10:00:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeTz()
    {
        // same with nextScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant lastScheduleTime = instant("2016-02-03 00:00:00 +0900");
            assertThat(
                    newScheduler("00 10 * * *", "Asia/Tokyo").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 10:00:00 +0900"),
                            instant("2016-02-03 10:00:00 +0900"))));
        }
    }

    @Test
    public void nextScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)

        //last is before DST, next in before DST
        {
            Instant lastScheduleTime = instant("2016-03-01 10:00:00 -0800");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-02 10:00:00 -0800"),
                            instant("2016-03-02 10:00:00 -0800"))));
        }
        //last is after DST, next in DST
        {
            Instant lastScheduleTime = instant("2016-03-14 10:00:00 -0700");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-15 10:00:00 -0700"),
                            instant("2016-03-15 10:00:00 -0700"))));
        }
        //last is before DST, next in DST
        {
            Instant lastScheduleTime = instant("2016-03-12 10:00:00 -0800");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 10:00:00 -0700"),
                            instant("2016-03-13 10:00:00 -0700"))));
        }
    }

    @Test
    public void lastScheduleTimeUtc()
    {
        {
            Instant currentScheduleTime = instant("2016-02-03 10:00:00 +0000");
            assertThat(
                    newScheduler("00 10 * * *", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-02 10:00:00 +0000"),
                            instant("2016-02-02 10:00:00 +0000"))));
        }
    }

    @Test
    public void lastScheduleTimeTz()
    {
        {
            Instant currentScheduleTime = instant("2016-02-03 10:00:00 +0900");
            assertThat(
                    newScheduler("00 10 * * *", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-02 10:00:00 +0900"),
                            instant("2016-02-02 10:00:00 +0900"))));
        }
    }

    @Test
    public void lastScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)

        // current at before DST(-0800), last will be at before DST
        {
            Instant currentScheduleTime = instant("2016-03-12 10:00:00 -0800");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-11 10:00:00 -0800"),
                            instant("2016-03-11 10:00:00 -0800"))));
        }
        // current at DST(-0700), last will be at DST
        {
            Instant currentScheduleTime = instant("2016-03-14 10:00:00 -0700");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 10:00:00 -0700"),
                            instant("2016-03-13 10:00:00 -0700"))));
        }
        // current at DST(-0700), last will be at before DST
        {
            Instant currentScheduleTime = instant("2016-03-13 10:00:00 -0700");
            assertThat(
                    newScheduler("00 10 * * *", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-12 10:00:00 -0800"),
                            instant("2016-03-12 10:00:00 -0800"))));
        }
    }
}
