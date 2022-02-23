package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;
import org.junit.Test;
import static java.util.Locale.ENGLISH;
import static io.digdag.client.DigdagClient.objectMapper;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DailySchedulerTest
{
    static Config newConfig()
    {
        return new ConfigFactory(objectMapper()).create();
    }

    static Scheduler newScheduler(String pattern, String timeZone)
    {
        return new DailySchedulerFactory().newScheduler(newConfig().set("_command", pattern), ZoneId.of(timeZone), Optional.absent(), Optional.absent());
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
        // current time is 09:00:00
        // schedule is 10:00:00 every day
        // schedule at today 10:00:00
        Instant currentTime1 = instant("2016-02-03 09:00:00 +0000");
        assertThat(
                newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime1),
                is(ScheduleTime.of(
                        instant("2016-02-03 00:00:00 +0000"),
                        instant("2016-02-03 10:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));

        // current time is 16:00:00
        // schedule is 10:00:00 every day
        // schedule at tomorrow 10:00:00
        Instant currentTime2 = instant("2016-02-03 16:00:00 +0000");
        assertThat(
                newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime2),
                is(ScheduleTime.of(
                        instant("2016-02-04 00:00:00 +0000"),
                        instant("2016-02-04 10:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void firstScheduleTimeTz()
    {
        // same with firstScheduleTimeUtc but with TZ=Asia/Tokyo

        Instant currentTime1 = instant("2016-02-03 09:00:00 +0900");
        assertThat(
                newScheduler("10:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime1),
                is(ScheduleTime.of(
                        instant("2016-02-03 00:00:00 +0900"),
                        instant("2016-02-03 10:00:00 +0900"),
                        Optional.absent(),
                        Optional.absent())));

        Instant currentTime2 = instant("2016-02-03 16:00:00 +0900");
        assertThat(
                newScheduler("10:00:00", "Asia/Tokyo").getFirstScheduleTime(currentTime2),
                is(ScheduleTime.of(
                        instant("2016-02-04 00:00:00 +0900"),
                        instant("2016-02-04 10:00:00 +0900"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void firstScheduleTimeDst()
    {
        // America/Los_Angeles begins DST at 2016-03-13 03:00:00 -0700
        // (== 2016-03-13 02:00:00 -0800)

        // This is an exceptional case with DST...
        // "daily> 10:00:00" means 10 hour later than 00:00:00.
        // 00:00:00 -08:00 plus 10 hours is 11:00:00 -07:00.
        Instant currentTime1 = instant("2016-03-13 09:00:00 -0700");
        assertThat(
                newScheduler("10:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime1),
                is(ScheduleTime.of(
                        instant("2016-03-13 00:00:00 -0800"),
                        instant("2016-03-13 11:00:00 -0700"), // schedule runs at 10:00:00 although definition is "daily> 10:00:00"
                        Optional.absent(),
                        Optional.absent())));

        Instant currentTime2 = instant("2016-03-13 16:00:00 -0700");
        assertThat(
                newScheduler("10:00:00", "America/Los_Angeles").getFirstScheduleTime(currentTime2),
                is(ScheduleTime.of(
                        instant("2016-03-14 00:00:00 -0700"),
                        instant("2016-03-14 10:00:00 -0700"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void firstScheduleTimeExact1()
    {
        Instant currentTime1 = instant("2016-03-13 00:00:00 +0000");
        assertThat(
                newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime1),
                is(ScheduleTime.of(
                        instant("2016-03-13 00:00:00 +0000"),
                        instant("2016-03-13 10:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));

        Instant currentTime2 = instant("2016-03-13 10:00:00 +0000");
        assertThat(
                newScheduler("10:00:00", "UTC").getFirstScheduleTime(currentTime2),
                is(ScheduleTime.of(
                        instant("2016-03-13 00:00:00 +0000"),
                        instant("2016-03-13 10:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));

        Instant currentTime3 = instant("2016-03-13 00:00:00 +0000");
        assertThat(
                newScheduler("00:00:00", "UTC").getFirstScheduleTime(currentTime3),
                is(ScheduleTime.of(
                        instant("2016-03-13 00:00:00 +0000"),
                        instant("2016-03-13 00:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void nextScheduleTimeUtc()
    {
        // last schedule time is 00:00:00
        // schedule is 10:00:00 every day
        // next schedule time is at tomorrow 00:00:00
        Instant lastScheduleTime1 = instant("2016-02-03 00:00:00 +0000");
        assertThat(
                newScheduler("10:00:00", "UTC").nextScheduleTime(lastScheduleTime1),
                is(ScheduleTime.of(
                        instant("2016-02-04 00:00:00 +0000"),
                        instant("2016-02-04 10:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void nextScheduleTimeTz()
    {
        // same with nextScheduleTimeUtc but with TZ=Asia/Tokyo

        Instant lastScheduleTime1 = instant("2016-02-03 00:00:00 +0900");
        assertThat(
                newScheduler("10:00:00", "Asia/Tokyo").nextScheduleTime(lastScheduleTime1),
                is(ScheduleTime.of(
                        instant("2016-02-04 00:00:00 +0900"),
                        instant("2016-02-04 10:00:00 +0900"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void nextScheduleTimeDst()
    {
        Instant lastScheduleTime1 = instant("2016-03-12 00:00:00 -0800");
        assertThat(
                newScheduler("10:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime1),
                is(ScheduleTime.of(
                        instant("2016-03-13 00:00:00 -0800"),
                        instant("2016-03-13 10:00:00 -0800"),
                        Optional.absent(),
                        Optional.absent())));

        Instant lastScheduleTime2 = instant("2016-03-13 00:00:00 -0800");
        assertThat(
                newScheduler("10:00:00", "America/Los_Angeles").nextScheduleTime(lastScheduleTime2),
                is(ScheduleTime.of(
                        instant("2016-03-14 00:00:00 -0700"),
                        instant("2016-03-14 10:00:00 -0700"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void lastScheduleTimeUtc()
    {
        // last schedule time is 00:00:00
        // schedule is 10:00:00 every day
        // next schedule time is at tomorrow 00:00:00
        Instant currentScheduleTime1 = instant("2016-02-03 00:00:00 +0000");
        assertThat(
                newScheduler("10:00:00", "UTC").lastScheduleTime(currentScheduleTime1),
                is(ScheduleTime.of(
                        instant("2016-02-02 00:00:00 +0000"),
                        instant("2016-02-02 10:00:00 +0000"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void lastScheduleTimeTz()
    {
        // same with lastScheduleTimeUtc but with TZ=Asia/Tokyo

        Instant currentScheduleTime1 = instant("2016-02-03 00:00:00 +0900");
        assertThat(
                newScheduler("10:00:00", "Asia/Tokyo").lastScheduleTime(currentScheduleTime1),
                is(ScheduleTime.of(
                        instant("2016-02-02 00:00:00 +0900"),
                        instant("2016-02-02 10:00:00 +0900"),
                        Optional.absent(),
                        Optional.absent())));
    }

    @Test
    public void lastScheduleTimeDst()
    {
        Instant currentScheduleTime1 = instant("2016-03-13 00:00:00 -0800");
        assertThat(
                newScheduler("10:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime1),
                is(ScheduleTime.of(
                        instant("2016-03-12 00:00:00 -0800"),
                        instant("2016-03-12 10:00:00 -0800"),
                        Optional.absent(),
                        Optional.absent())));

        Instant currentScheduleTime2 = instant("2016-03-14 00:00:00 -0700");
        assertThat(
                newScheduler("10:00:00", "America/Los_Angeles").lastScheduleTime(currentScheduleTime2),
                is(ScheduleTime.of(
                        instant("2016-03-13 00:00:00 -0800"),
                        instant("2016-03-13 10:00:00 -0800"),
                        Optional.absent(),
                        Optional.absent())));
    }
}
