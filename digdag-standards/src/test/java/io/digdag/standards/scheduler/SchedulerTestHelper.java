package io.digdag.standards.scheduler;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Scheduler;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import static io.digdag.client.DigdagClient.objectMapper;
import static java.util.Locale.ENGLISH;

abstract class SchedulerTestHelper
{
    final ScheduleConfigHelper configHelper = new ScheduleConfigHelper();

    private static DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH);

    static Config newConfig()
    {
        return new ConfigFactory(objectMapper()).create();
    }

    static Config newConfig(String command, Optional<String> start, Optional<String> end)
    {
        Config config = new ConfigFactory(objectMapper()).create().set("_command", command);
        config = start.isPresent() ? config.set("start", start.get()) : config;
        config = end.isPresent() ? config.set("end", end.get()) : config;
        return config;
    }

    static Instant instant(String time)
    {
        return Instant.from(TIME_FORMAT.parse(time));
    }

    Scheduler newScheduler(String pattern, String timeZone)
    {
        return newScheduler(pattern, timeZone, Optional.absent(), Optional.absent());
    }

    abstract Scheduler newScheduler(String pattern, String timeZone, Optional<String> start, Optional<String> end);
}
