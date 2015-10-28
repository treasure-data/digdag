package io.digdag.core;

public interface SchedulerFactory
{
    boolean matches(ConfigSource config);

    Scheduler newScheduler(ConfigSource config);
}
