package io.digdag.core;

import io.digdag.core.config.Config;

public interface SchedulerFactory
{
    boolean matches(Config config);

    Scheduler newScheduler(Config config);
}
