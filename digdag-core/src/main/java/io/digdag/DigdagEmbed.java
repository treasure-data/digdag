package io.digdag;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.Scopes;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import io.digdag.core.*;

public class DigdagEmbed
{
    public static class Bootstrap
    {
        public DigdagEmbed initialize()
        {
            return new DigdagEmbed();
        }
    }

    private final Injector injector;

    DigdagEmbed()
    {
        this.injector = Guice.createInjector(
                new ObjectMapperModule()
                    .registerModule(new GuavaModule())
                    .registerModule(new JodaModule()),
                    new DatabaseModule(DatabaseStoreConfig.builder()
                        .type("h2")
                        //.url("jdbc:h2:../test")
                        .url("jdbc:h2:mem:test")
                        .build()),
                (binder) -> {
                    binder.bind(TaskApi.class).to(InProcessTaskApi.class).in(Scopes.SINGLETON);
                    binder.bind(ConfigSourceFactory.class).in(Scopes.SINGLETON);
                    binder.bind(ConfigSourceMapper.class).in(Scopes.SINGLETON);
                    binder.bind(DatabaseMigrator.class).in(Scopes.SINGLETON);
                    binder.bind(SessionExecutor.class).in(Scopes.SINGLETON);
                    binder.bind(YamlConfigLoader.class).in(Scopes.SINGLETON);
                    binder.bind(TaskQueueDispatcher.class).in(Scopes.SINGLETON);
                    binder.bind(ScheduleStarter.class).to(StandardScheduleStarter.class).in(Scopes.SINGLETON);
                    binder.bind(LocalAgentManager.class).in(Scopes.SINGLETON);
                    binder.bind(SchedulerManager.class).in(Scopes.SINGLETON);
                    binder.bind(LocalSite.class).in(Scopes.SINGLETON);

                    Multibinder<TaskQueueFactory> taskQueueBinder = Multibinder.newSetBinder(binder, TaskQueueFactory.class);
                    taskQueueBinder.addBinding().to(MemoryTaskQueueFactory.class).in(Scopes.SINGLETON);

                    Multibinder<TaskExecutorFactory> taskExecutorBinder = Multibinder.newSetBinder(binder, TaskExecutorFactory.class);
                    taskExecutorBinder.addBinding().to(PyTaskExecutorFactory.class).in(Scopes.SINGLETON);
                    taskExecutorBinder.addBinding().to(ShTaskExecutorFactory.class).in(Scopes.SINGLETON);

                    Multibinder<SchedulerFactory> schedulerBinder = Multibinder.newSetBinder(binder, SchedulerFactory.class);
                    schedulerBinder.addBinding().to(CronSchedulerFactory.class).in(Scopes.SINGLETON);
                }
            );
    }

    public Injector getInjector()
    {
        return injector;
    }
}
