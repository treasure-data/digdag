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
    private static DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH);

    static Config newConfig(String command)
    {
        Config config = new ConfigFactory(objectMapper()).create().set("_command", command);
        return config;
    }

    static Instant instant(String time)
    {
        return Instant.from(TIME_FORMAT.parse(time));
    }

    abstract Scheduler newScheduler(String pattern, String timeZone);
}
