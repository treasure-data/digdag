package io.digdag.spi;

import java.time.Instant;
import java.time.ZoneId;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;

public interface SchedulerFactory
{
    String getType();

    Scheduler newScheduler(Config config, ZoneId timeZone, Optional<Instant> startDate, Optional<Instant> endDate);
}
