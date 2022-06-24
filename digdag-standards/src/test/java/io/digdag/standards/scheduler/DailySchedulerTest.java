package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.google.common.base.Optional;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;
import org.junit.Test;
import static java.util.Locale.ENGLISH;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DailySchedulerTest extends SchedulerTestHelper
{
    Scheduler newScheduler(String pattern, String timeZone, Optional<String> start, Optional<String> end)
    {
        return new DailySchedulerFactory(configHelper).newScheduler(newConfig(pattern, start, end), ZoneId.of(timeZone));
    }

    private static DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH);

    static Instant instant(String time)
    {
        return Instant.from(TIME_FORMAT.parse(time));
    }

    @Test
    public void firstScheduleTimeUtc()
    {
        {
            Instant currentTime = instant("2016-02-03 09:34:12 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 10:00:00 +0000"))));
        }
        {
            Instant currentTime = instant("2016-02-03 10:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0000"),
                            instant("2016-02-03 10:00:00 +0000"))));
        }
        {
            Instant currentTime = instant("2016-02-03 10:00:01 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-04 00:00:00 +0000"),
                            instant("2016-02-04 10:00:00 +0000"))));
        }
    }

    @Test
    public void firstScheduleTimeTz()
    {
        // same with firstScheduleTimeUtc but with TZ=Asia/Tokyo (+0900)
        {
            Instant currentTime = instant("2016-02-03 09:34:12 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 10:00:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-03 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-03 00:00:00 +0900"),
                            instant("2016-02-03 10:00:00 +0900"))));
        }
        {
            Instant currentTime = instant("2016-02-03 10:00:01 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-02-04 00:00:00 +0900"),
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
            Instant currentTime = instant("2016-03-11 12:34:56 -0800");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-12 00:00:00 -0800"),
                            instant("2016-03-12 10:00:00 -0800"))));
        }
        // Current is at DST, next first be at DST
        {
            Instant currentTime = instant("2016-03-13 16:31:03 -0700");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-14 00:00:00 -0700"),
                            instant("2016-03-14 10:00:00 -0700"))));
        }

        // Current is at DST, first session time will be at before DST, run time will be at DST
        // "daily> 10:00:00" means 10 hour later than 00:00:00.
        // 00:00:00 -08:00 plus 10 hours is 11:00:00 -07:00.
        {
            Instant currentTime = instant("2016-03-13 09:00:00 -0700");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 -0800"),
                            instant("2016-03-13 11:00:00 -0700"))));
        }

        // Current is at before DST, first session time will be at before DST, run time will be at DST
        {
            Instant currentTime = instant("2016-03-13 01:59:31 -0800");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 -0800"),
                            instant("2016-03-13 11:00:00 -0700"))));
        }
    }

    @Test
    public void firstScheduleTimeMisc()
    {
        // boundary test: current time is just 00:00:00
        {
            Instant currentTime = instant("2016-03-13 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 +0000"),
                            instant("2016-03-13 10:00:00 +0000"))));
        }
        // boundary test: current time is same with "dail>: 10:00:00"
        {
            Instant currentTime = instant("2016-03-13 10:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 +0000"),
                            instant("2016-03-13 10:00:00 +0000"))));
        }
        // boundary test: schedule is set to 00:00:00
        {
            Instant currentTime = instant("2016-03-13 00:00:00 +0000");
            assertThat(
                    newScheduler("00:00:00", "UTC").getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 +0000"),
                            instant("2016-03-13 00:00:00 +0000"))));
        }
    }

    @Test
    public void firstScheduleTimeStartEnd() {
        // check start
        {
            Instant currentTime = instant("2016-02-03 09:59:59 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC", Optional.of("2016-03-01"), Optional.of("2016-03-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0000"),
                            instant("2016-03-01 10:00:00 +0000"))));
        }
        // check start
        {
            Instant currentTime = instant("2016-02-03 09:59:59 +0000");
            assertThat(
                    newScheduler("23:59:59", "UTC", Optional.of("2016-03-01"), Optional.of("2016-03-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0000"),
                            instant("2016-03-01 23:59:59 +0000"))));
        }
        // check end
        {
            Instant currentTime = instant("2016-03-31 10:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC", Optional.of("2016-03-01"), Optional.of("2016-03-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("2016-03-31 00:00:00 +0000"),
                            instant("2016-03-31 10:00:00 +0000"))));
        }
        // check end
        {
            Instant currentTime = instant("2016-03-31 10:00:01 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC", Optional.of("2016-03-01"), Optional.of("2016-03-31")).getFirstScheduleTime(currentTime),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeUtc() {
        // last schedule time is 00:00:00
        // schedule is 10:00:00 every day
        // next schedule time is at tomorrow 00:00:00
        {
            Instant lastScheduleTime = instant("2016-02-03 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-04 00:00:00 +0000"),
                            instant("2016-02-04 10:00:00 +0000"))));
        }
        {
            Instant lastScheduleTime = instant("2016-01-31 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0000"),
                            instant("2016-02-01 10:00:00 +0000"))));
        }
        {
            Instant lastScheduleTime = instant("2015-12-31 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0000"),
                            instant("2016-01-01 10:00:00 +0000"))));
        }
    }

    @Test
    public void nextScheduleTimeTz()
    {
        // same with nextScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant lastScheduleTime1 = instant("2016-02-03 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-02-04 00:00:00 +0900"),
                            instant("2016-02-04 10:00:00 +0900"))));
        }
        {
            Instant lastScheduleTime = instant("2016-01-31 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0900"),
                            instant("2016-02-01 10:00:00 +0900"))));
        }
        {
            Instant lastScheduleTime = instant("2015-12-31 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-01 00:00:00 +0900"),
                            instant("2016-01-01 10:00:00 +0900"))));
        }
    }

    @Test
    public void nextScheduleTimeDst()
    {
        {
            Instant lastScheduleTime1 = instant("2016-03-12 00:00:00 -0800");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 -0800"),
                            instant("2016-03-13 10:00:00 -0800"))));
        }
        {
            Instant lastScheduleTime2 = instant("2016-03-13 00:00:00 -0800");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime2),
                    is(ScheduleTime.of(
                            instant("2016-03-14 00:00:00 -0700"),
                            instant("2016-03-14 10:00:00 -0700"))));
        }
    }

    @Test
    public void nextScheduleTimeMisc() {
        //last schedule does not comply with the rule "YYYY-MM-DD 00:00:00"
        {
            Instant lastScheduleTime = instant("2016-03-02 12:34:56 -0800");
            assertThat(
                    newScheduler("10:11:23", "America/Los_Angeles").nextScheduleTime(lastScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-03 00:00:00 -0800"),
                            instant("2016-03-03 10:11:23 -0800"))));
        }
    }

    @Test
    public void nextScheduleTimeStartEnd()
    {
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-02-03 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 10:00:00 +0900"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-02-29 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 10:00:00 +0900"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-03-01 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-02 00:00:00 +0900"),
                            instant("2016-03-02 10:00:00 +0900"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-03-30 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("2016-03-31 00:00:00 +0900"),
                            instant("2016-03-31 10:00:00 +0900"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-03-31 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
        // check start
        {
            Instant lastScheduleTime1 = instant("2016-04-01 10:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).nextScheduleTime(lastScheduleTime1),
                    is(ScheduleTime.of(
                            instant("9999-01-01 00:00:00 +0000"),
                            instant("9999-01-01 00:00:00 +0000"))));
        }
    }

    @Test
    public void lastScheduleTimeUtc()
    {
        {
            Instant currentScheduleTime = instant("2016-02-02 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0000"),
                            instant("2016-02-01 10:00:00 +0000"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-01 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-31 00:00:00 +0000"),
                            instant("2016-01-31 10:00:00 +0000"))));
        }
        {
            Instant currentScheduleTime = instant("2016-01-01 00:00:00 +0000");
            assertThat(
                    newScheduler("10:00:00", "UTC").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2015-12-31 00:00:00 +0000"),
                            instant("2015-12-31 10:00:00 +0000"))));
        }
    }

    @Test
    public void lastScheduleTimeTz()
    {
        // same with lastScheduleTimeUtc but with TZ=Asia/Tokyo
        {
            Instant currentScheduleTime = instant("2016-02-02 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-01 00:00:00 +0900"),
                            instant("2016-02-01 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-02-01 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-01-31 00:00:00 +0900"),
                            instant("2016-01-31 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-01-01 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2015-12-31 00:00:00 +0900"),
                            instant("2015-12-31 10:00:00 +0900"))));
        }
    }

    @Test
    public void lastScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)

        // current at before DST(-0800), last will be at before DST
        {
            Instant currentScheduleTime = instant("2016-03-13 00:00:00 -0800");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-12 00:00:00 -0800"),
                            instant("2016-03-12 10:00:00 -0800"))));
        }

        // current at DST(-0700), last will be at DST
        {
            Instant currentScheduleTime = instant("2016-03-15 00:00:00 -0700");
            assertThat(
                    newScheduler("01:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-14 00:00:00 -0700"),
                            instant("2016-03-14 01:00:00 -0700"))));
        }

        // current at DST(-0700), last will be at before DST(-0800)
        {
            Instant currentScheduleTime = instant("2016-03-14 00:00:00 -0700");
            assertThat(
                    newScheduler("01:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 -0800"),
                            instant("2016-03-13 01:00:00 -0800"))));
        }

        // current at DST(-0700), last session time will be at before DST(-0800), run time will be at DST(-0700)
        {
            Instant currentScheduleTime = instant("2016-03-14 00:00:00 -0700");
            assertThat(
                    newScheduler("05:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-13 00:00:00 -0800"),
                            instant("2016-03-13 06:00:00 -0700"))));
        }
    }

    @Test
    public void lastScheduleTimeMisc()
    {
        // current schedule time does not comply with Monthly>'s one = 'YYYY-MM-DD 00:00:00'
        {
            Instant currentScheduleTime = instant("2016-09-13 12:34:56 -0700");
            assertThat(
                    newScheduler("10:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-09-13 00:00:00 -0700"),
                            instant("2016-09-13 10:00:00 -0700"))));
        }
    }

    @Test
    public void lastScheduleTimeStartEnd()
    {
        // lastScheduleTime calculation ignore start/end

        {
            Instant currentScheduleTime = instant("2016-02-29 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-28 00:00:00 +0900"),
                            instant("2016-02-28 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-03-01 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-02-29 00:00:00 +0900"),
                            instant("2016-02-29 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-03-02 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-01 00:00:00 +0900"),
                            instant("2016-03-01 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-03-31 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-30 00:00:00 +0900"),
                            instant("2016-03-30 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-04-01 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-03-31 00:00:00 +0900"),
                            instant("2016-03-31 10:00:00 +0900"))));
        }
        {
            Instant currentScheduleTime = instant("2016-04-02 00:00:00 +0900");
            assertThat(
                    newScheduler("10:00:00", "Asia/Tokyo", Optional.of("2016-03-01"), Optional.of("2016-03-31")).lastScheduleTime(currentScheduleTime),
                    is(ScheduleTime.of(
                            instant("2016-04-01 00:00:00 +0900"),
                            instant("2016-04-01 10:00:00 +0900"))));
        }
    }
}
