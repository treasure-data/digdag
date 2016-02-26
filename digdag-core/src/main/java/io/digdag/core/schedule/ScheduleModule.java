package io.digdag.core.schedule;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.digdag.core.session.SessionMonitorExecutor;

public class ScheduleModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(SchedulerManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleHandler.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleExecutor.class).in(Scopes.SINGLETON);
        binder.bind(SlaCalculator.class).in(Scopes.SINGLETON);

        // session
        binder.bind(SessionMonitorExecutor.class).in(Scopes.SINGLETON);
    }
}
