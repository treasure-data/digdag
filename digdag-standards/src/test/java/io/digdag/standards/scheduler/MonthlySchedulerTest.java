package io.digdag.standards.scheduler;

import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MonthlySchedulerTest extends SchedulerTestHelper
{
    Scheduler newScheduler(String pattern, String timeZone, Optional<String> start, Optional<String> end)
    {
        return new MonthlySchedulerFactory(configHelper).newScheduler(newConfig(pattern, start, end), ZoneId.of(timeZone));
    }

    @Test
    public void firstScheduleTimeUtc()
    {
        {
            Instant currentTime = instant("2016-02-01 11:59:59 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0000"),
                            instant("2016-02-01 12:00:00 +0000"))));
        }
        {
            Instant currentTime = instant("2016-02-01 12:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0000"),
                            instant("2016-02-01 12:00:00 +0000"))));
        }
        {
            Instant currentTime = instant("2016-02-01 12:00:01 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0000"),
                            instant("2016-03-01 12:00:00 +0000"))));
        }
    }

    @Test
    public void firstScheduleTimeTz()
    {
        // same with firstScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant currentTime = instant("2016-02-01 11:59:59 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0900"),
                            instant("2016-02-01 12:00:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-01 12:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0900"),
                            instant("2016-02-01 12:00:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-01 12:00:01 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 12:00:00 +0900"))));
        }
    }

    @Test
    public void firstScheduleTimeDst()
    {
        // America/Los_Angeles(-0800) begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)
        // Current is at before DST, first will be at before DST
        {
            Instant currentTime = instant("2016-02-12 06:12:34 -0800");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 -0800"),
                            instant("2016-03-01 12:00:00 -0800"))));
        }
        // Current is at DST, next first be at DST
        {
            Instant currentTime = instant("2016-03-13 16:00:00 -0700");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 -0700"),
                            instant("2016-04-01 12:00:00 -0700"))));
        }
        // Current is at before DST, first will be at DST
        {
            Instant currentTime = instant("2016-03-13 01:59:00 -0800");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 -0700"),
                            instant("2016-04-01 12:00:00 -0700"))));
        }
    }

    @Test
    public void firstScheduleTimeMisc()
    {
        //CurrentTime with boundary value 00:00:00
        {
            Instant currentTime = instant("2016-03-13 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 +0000"),
                            instant("2016-04-01 12:00:00 +0000"))));
        }
        //Next year
        {
            Instant currentTime = instant("2016-12-03 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2017-01-01 00:00:00 +0000"),
                            instant("2017-01-01 12:00:00 +0000"))));
        }
        //Non existent day 02-31
        {
            Instant currentTime = instant("2016-01-31 19:12:34 +0000");
            // 02-31 does not exist. So skip to 03-31. Strange.
            assertThat(
                    newScheduler("31,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-31 00:00:00 +0000"),
                            instant("2016-03-31 12:00:00 +0000"))));
        }
        //Non existent day 06-31
        {
            Instant currentTime = instant("2016-05-31 19:12:34 +0000");
            // 06-31 does not exist. So skip to 07-31.
            assertThat(
                    newScheduler("31,12:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-07-31 00:00:00 +0000"),
                            instant("2016-07-31 12:00:00 +0000"))));
        }
    }

    @Test
    public void firstScheduleTimeStartEnd()
    {
        // check start
        {
            Instant currentTime = instant("2016-02-29 23:59:59 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 12:00:00 +0900"))));
        }
        // check start
        {
            Instant currentTime = instant("2016-03-01 12:00:01 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 +0900"),
                            instant("2016-04-01 12:00:00 +0900"))));
        }
        // check start
        {
            Instant currentTime = instant("2016-02-29 23:59:59 +0900");
            assertThat(
                    newScheduler("31,23:59:59", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-31 00:00:00 +0900"),
                            instant("2016-03-31 23:59:59 +0900"))));
        }
        // check end
        {
            Instant currentTime = instant("2016-05-01 12:00:01 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
        // check end
        {
            Instant currentTime = instant("2016-06-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeUtc()
    {
        {
            Instant lastScheduleTime = instant("2016-02-01 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0000"),
                            instant("2016-03-01 12:00:00 +0000"))));
        }
        {
            Instant lastScheduleTime = instant("2015-12-01 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0000"),
                            instant("2016-01-01 12:00:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeTz()
    {
        // same with nextScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant lastScheduleTime1 = instant("2016-02-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 12:00:00 +0900"))));
        }
        {
            Instant lastScheduleTime1 = instant("2015-12-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0900"),
                            instant("2016-01-01 12:00:00 +0900"))));
        }
    }

    @Test
    public void nextScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)
        //last is before DST, next in before DST
        {
            Instant lastScheduleTime = instant("2016-01-01 00:00:00 -0800");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 -0800"),
                            instant("2016-02-01 12:00:00 -0800"))));
        }
        //last is after DST, next in DST
        {
            Instant lastScheduleTime = instant("2016-04-01 00:00:00 -0700");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-05-01 00:00:00 -0700"),
                            instant("2016-05-01 12:00:00 -0700"))));
        }
        //last is before DST, next in DST
        {
            Instant lastScheduleTime = instant("2016-03-01 00:00:00 -0800");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 -0700"),
                            instant("2016-04-01 12:00:00 -0700"))));
        }
    }

    @Test
    public void nextScheduleTimeMisc() {
        //last schedule does not comply with the rule "YYYY-MM-DD 00:00:00"
        {
            Instant lastScheduleTime = instant("2016-03-13 12:34:56 -0700");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 -0700"),
                            instant("2016-04-01 12:00:00 -0700"))));
        }
        //Non existent day 02-31
        {
            Instant lastScheduleTime = instant("2016-01-31 00:00:00 -0800");
            assertThat(
                    newScheduler("31,12:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-31 00:00:00 -0700"),
                            instant("2016-03-31 12:00:00 -0700"))));
        }
        //Non existent day 06-31
        {
            Instant lastScheduleTime = instant("2016-05-31 00:00:00 -0800");
            assertThat(
                    newScheduler("31,12:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-07-31 00:00:00 -0700"),
                            instant("2016-07-31 12:00:00 -0700"))));
        }
    }

    @Test
    public void nextScheduleTimeStartEnd()
    {
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-01-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 12:00:00 +0900"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-02-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 12:00:00 +0900"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-03-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 +0900"),
                            instant("2016-04-01 12:00:00 +0900"))));
        }
        // check end
        {
            Instant lastScheduleTime1 = instant("2016-05-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
        // check end
        {
            Instant lastScheduleTime1 = instant("2016-06-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
    }

    @Test
    public void lastScheduleTimeUtc()
    {
        {
            Instant currentScheduleTime = instant("2016-02-01 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0000"),
                            instant("2016-01-01 12:00:00 +0000"))));
        }
        {
            Instant currentScheduleTime = instant("2016-01-31 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0000"),
                            instant("2016-01-01 12:00:00 +0000"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-02 00:00:00 +0000");
            assertThat(
                    newScheduler("1,12:00:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0000"),
                            instant("2016-02-01 12:00:00 +0000"))));
        }
    }

    @Test
    public void lastScheduleTimeTz()
    {
        //2016-02-09:Tue 02-10:Wed 02-11:Thu
        {
            Instant currentScheduleTime = instant("2016-02-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0900"),
                            instant("2016-01-01 12:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-01-31 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0900"),
                            instant("2016-01-01 12:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-02 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0900"),
                            instant("2016-02-01 12:00:00 +0900"))));
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
            Instant currentScheduleTime = instant("2016-03-01 00:00:00 -0800");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 -0800"),
                            instant("2016-02-01 12:00:00 -0800"))));
        }

        // current at DST(-0700), last will be at DST
        {
            Instant currentScheduleTime2 = instant("2016-05-01 00:00:00 -0700");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime2),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 -0700"),
                            instant("2016-04-01 12:00:00 -0700"))));
        }

        // current at DST(-0700), last will be at before DST(-0800)
        {
            Instant currentScheduleTime3 = instant("2016-04-01 00:00:00 -0700");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime3),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 -0800"),
                            instant("2016-03-01 12:00:00 -0800"))));
        }
    }

    @Test
    public void lastScheduleTimeMisc()
    {
        // current schedule time does not comply with Monthly>'s one = 'YYYY-MM-DD 00:00:00'
        {
            Instant currentScheduleTime = instant("2016-09-13 12:34:56 -0700");
            assertThat(
                    newScheduler("1,12:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-09-01 00:00:00 -0700"),
                            instant("2016-09-01 12:00:00 -0700"))));
        }
    }

    @Test
    public void lastScheduleTimeStartEnd()
    {
        // lastScheduleTime calculation ignore start/end
        {
            Instant currentScheduleTime = instant("2016-02-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0900"),
                            instant("2016-01-01 12:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-03-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0900"),
                            instant("2016-02-01 12:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-04-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 12:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-06-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-05-01 00:00:00 +0900"),
                            instant("2016-05-01 12:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-07-01 00:00:00 +0900");
            assertThat(
                    newScheduler("1,12:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-05-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-06-01 00:00:00 +0900"),
                            instant("2016-06-01 12:00:00 +0900"))));
        }
    }
}
