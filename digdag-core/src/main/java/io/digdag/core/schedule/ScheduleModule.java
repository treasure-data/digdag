package io.digdag.core.schedule;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import io.digdag.spi.SchedulerFactory;

public class ScheduleModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(SchedulerManager.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder, SchedulerFactory.class);
    }
}
