package io.digdag.standards.scheduler;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.SchedulerFactory;
import io.digdag.standards.scheduler.CronSchedulerFactory;
import io.digdag.standards.scheduler.MonthlySchedulerFactory;
import io.digdag.standards.scheduler.WeeklySchedulerFactory;
import io.digdag.standards.scheduler.DailySchedulerFactory;
import io.digdag.standards.scheduler.HourlySchedulerFactory;
import io.digdag.standards.scheduler.MinutesIntervalSchedulerFactory;
import io.digdag.standards.scheduler.QuartzCronSchedulerFactory;

public class SchedulerModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        addStandardSchedulerFactory(binder, CronSchedulerFactory.class);
        addStandardSchedulerFactory(binder, QuartzCronSchedulerFactory.class);
        addStandardSchedulerFactory(binder, MonthlySchedulerFactory.class);
        addStandardSchedulerFactory(binder, WeeklySchedulerFactory.class);
        addStandardSchedulerFactory(binder, DailySchedulerFactory.class);
        addStandardSchedulerFactory(binder, HourlySchedulerFactory.class);
        addStandardSchedulerFactory(binder, MinutesIntervalSchedulerFactory.class);
        addStandardSchedulerFactory(binder, SecondsIntervalSchedulerFactory.class);
    }

    protected void addStandardSchedulerFactory(Binder binder, Class<? extends SchedulerFactory> factory)
    {
        Multibinder.newSetBinder(binder, SchedulerFactory.class)
            .addBinding().to(factory).in(Scopes.SINGLETON);
    }
}
