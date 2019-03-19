package io.digdag.core.schedule;

import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;

public class ScheduleExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ScheduleConfig.class).toProvider(ScheduleConfigProvider.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleExecutor.class).asEagerSingleton();
    }
}
