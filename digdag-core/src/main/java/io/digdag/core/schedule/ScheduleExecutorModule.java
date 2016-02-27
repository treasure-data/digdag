package io.digdag.core.schedule;

import javax.annotation.PostConstruct;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;

public class ScheduleExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ScheduleHandler.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleExecutor.class).in(Scopes.SINGLETON);

        binder.bind(ScheduleExecutorStarter.class).asEagerSingleton();
    }

    public static class ScheduleExecutorStarter
    {
        private final ScheduleExecutor scheduleExecutor;

        @Inject
        public ScheduleExecutorStarter(
                ScheduleExecutor scheduleExecutor)
        {
            this.scheduleExecutor = scheduleExecutor;
        }

        @PostConstruct
        public void start()
        {
            scheduleExecutor.start();
        }
    }
}
