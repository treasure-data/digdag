package io.digdag.standards.scheduler;

import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WeeklySchedulerTest extends SchedulerTestHelper
{
    Scheduler newScheduler(String pattern, String timeZone)
    {
        return new WeeklySchedulerFactory().newScheduler(newConfig(pattern), ZoneId.of(timeZone));
    }

    @Test
    public void firstScheduleTimeUtc()
    {
        // 2016-02-03 Wed.
        {
            Instant currentTime = instant("2016-02-03 17:14:59 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 17:15:00 +0000"))));
        }
        {
            Instant currentTime = instant("2016-02-03 17:15:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 17:15:00 +0000"))));
        }
        {
            Instant currentTime = instant("2016-02-03 17:15:01 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0000"),
                            instant("2016-02-10 17:15:00 +0000"))));
        }
    }

    @Test
    public void firstScheduleTimeTz()
    {
        // same with firstScheduleTimeUtc but with TZ=Asia/Tokyo
        // 2016-02-03 Wed.
        {
            Instant currentTime = instant("2016-02-03 17:14:59 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 17:15:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-03 17:15:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 17:15:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-03 17:15:01 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0900"),
                            instant("2016-02-10 17:15:00 +0900"))));
        }
    }

    @Test
    public void firstScheduleTimeDst()
    {
        // America/Los_Angeles(-0800) begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)
        // 2016-03-13 Sun
        // Current is at before DST, first will be at before DST
        {
            Instant currentTime = instant("2016-03-12 00:00:00 -0800");
            assertThat(
                    newScheduler("Sat,15:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-12 00:00:00 -0800"),
                            instant("2016-03-12 15:00:00 -0800"))));
        }
        // Current is at DST, next first be at DST
        {
            Instant currentTime = instant("2016-03-13 16:00:00 -0700");
            assertThat(
                    newScheduler("Sat,15:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-19 00:00:00 -0700"),
                            instant("2016-03-19 15:00:00 -0700"))));
        }
        // Current is at before DST, first will be at DST
        {
            Instant currentTime = instant("2016-03-13 01:59:00 -0800");
            assertThat(
                    newScheduler("Sun,15:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 01:00:00 -0700"),
                            instant("2016-03-13 16:00:00 -0700"))));
        }
    }

    @Test
    public void firstScheduleTimeMisc()
    {
        //Test for the currentTime with boundary value 00:00:00
        {
            Instant currentTime = instant("2016-03-13 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-16 00:00:00 +0000"),
                            instant("2016-03-16 17:15:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeUtc()
    {
        //2016-02-002:Tue 02-03:Wed 02-04:Thu.
        {
            Instant lastScheduleTime = instant("2016-02-02 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 17:15:00 +0000"))));
        }
        {
            Instant lastScheduleTime = instant("2016-02-03 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0000"),
                            instant("2016-02-10 17:15:00 +0000"))));
        }
        {
            Instant lastScheduleTime = instant("2016-02-04 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0000"),
                            instant("2016-02-10 17:15:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeTz()
    {
        // same with nextScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant lastScheduleTime1 = instant("2016-02-02 00:00:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 17:15:00 +0900"))));
        }
        {
            Instant lastScheduleTime1 = instant("2016-02-03 00:00:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0900"),
                            instant("2016-02-10 17:15:00 +0900"))));
        }
        {
            Instant lastScheduleTime1 = instant("2016-02-04 00:00:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0900"),
                            instant("2016-02-10 17:15:00 +0900"))));
        }
    }

    @Test
    public void nextScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)
        // 2016-03-12 Sat.
        //last is before DST, next in before DST
        {
            Instant lastScheduleTime = instant("2016-03-05 00:00:00 -0800");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-12 00:00:00 -0800"),
                            instant("2016-03-12 17:15:00 -0800"))));
        }
        //last is after DST, next in DST
        {
            Instant lastScheduleTime = instant("2016-03-19 00:00:00 -0700");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-26 00:00:00 -0700"),
                            instant("2016-03-26 17:15:00 -0700"))));
        }
        //last is before DST, next in DST
        {
            Instant lastScheduleTime = instant("2016-03-12 00:00:00 -0800");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-19 00:00:00 -0700"),
                            instant("2016-03-19 17:15:00 -0700"))));
        }
    }

    @Test
    public void nextScheduleTimeMisc() {
        //last schedule does not comply with the rule "YYYY-MM-DD 00:00:00"
        //2016-03-13:Sun
        {
            Instant lastScheduleTime = instant("2016-03-13 12:34:56 -0700");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-19 00:00:00 -0700"),
                            instant("2016-03-19 17:15:00 -0700"))));
        }
    }

    @Test
    public void lastScheduleTimeUtc()
    {
        //2016-02-09:Tue 02-10:Wed 02-11:Thu
        {
            Instant currentScheduleTime = instant("2016-02-09 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 17:15:00 +0000"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-10 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 17:15:00 +0000"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-11 00:00:00 +0000");
            assertThat(
                    newScheduler("Wed,17:15:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0000"),
                            instant("2016-02-10 17:15:00 +0000"))));
        }
    }

    @Test
    public void lastScheduleTimeTz()
    {
        //2016-02-09:Tue 02-10:Wed 02-11:Thu
        {
            Instant currentScheduleTime = instant("2016-02-09 00:00:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 17:15:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-10 00:00:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 17:15:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-11 00:00:00 +0900");
            assertThat(
                    newScheduler("Wed,17:15:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-10 00:00:00 +0900"),
                            instant("2016-02-10 17:15:00 +0900"))));
        }
    }

    @Test
    public void lastScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)
        // 2016-03-12:Sat

        // current at before DST(-0800), last will be at before DST
        {
            Instant currentScheduleTime = instant("2016-03-12 00:00:00 -0800");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-05 00:00:00 -0800"),
                            instant("2016-03-05 17:15:00 -0800"))));
        }

        // current at DST(-0700), last will be at DST
        {
            Instant currentScheduleTime2 = instant("2016-03-26 00:00:00 -0700");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime2),
                    is(ScheduleTime.of(
                            instant("2016-03-19 00:00:00 -0700"),
                            instant("2016-03-19 17:15:00 -0700"))));
        }

        // current at DST(-0700), last will be at before DST(-0800)
        {
            Instant currentScheduleTime3 = instant("2016-03-19 00:00:00 -0700");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime3),
                    is(ScheduleTime.of(
                            instant("2016-03-12 00:00:00 -0800"),
                            instant("2016-03-12 17:15:00 -0800"))));
        }
    }

    @Test
    public void lastScheduleTimeMisc()
    {
        // current schedule time does not comply with hourly>'s one = 'YYYY-MM-DD 00:00:00'
        // 2016-03-13:Sun
        {
            Instant currentScheduleTime = instant("2016-03-13 12:34:56 -0700");
            assertThat(
                    newScheduler("Sat,17:15:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-12 00:00:00 -0800"),
                            instant("2016-03-12 17:15:00 -0800"))));
        }
    }
}
