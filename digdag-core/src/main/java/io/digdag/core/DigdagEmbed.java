package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.core.agent.LocalAgentManager;
import io.digdag.core.agent.TaskRunnerManager;
import io.digdag.core.queue.TaskQueueManager;
import io.digdag.core.database.ConfigMapper;
import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.database.DatabaseModule;
import io.digdag.core.database.DatabaseStoreConfig;
import io.digdag.core.schedule.ScheduleStarter;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.session.SessionMonitorExecutor;
import io.digdag.core.session.SessionMonitorManager;
import io.digdag.core.workflow.InProcessTaskCallbackApi;
import io.digdag.core.workflow.TaskCallbackApi;
import io.digdag.core.workflow.TaskQueueDispatcher;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowExecutor;
import org.embulk.guice.LifeCycleInjector;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
//import com.fasterxml.jackson.datatype.joda.JodaModule;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.yaml.YamlConfigLoader;

public class DigdagEmbed
{
    public static class Bootstrap
    {
        private final List<Function<? super List<Module>, ? extends Iterable<? extends Module>>> moduleOverrides = new ArrayList<>();

        public Bootstrap addModules(Module... additionalModules)
        {
            return addModules(Arrays.asList(additionalModules));
        }

        public Bootstrap addModules(Iterable<? extends Module> additionalModules)
        {
            final List<Module> copy = ImmutableList.copyOf(additionalModules);
            return overrideModules(modules -> Iterables.concat(modules, copy));
        }

        public Bootstrap overrideModules(Function<? super List<Module>, ? extends Iterable<? extends Module>> function)
        {
            moduleOverrides.add(function);
            return this;
        }

        public DigdagEmbed initialize()
        {
            return build(true);
        }

        public DigdagEmbed initializeCloseable()
        {
            return build(false);
        }

        private DigdagEmbed build(boolean destroyOnShutdownHook)
        {
            final org.embulk.guice.Bootstrap bootstrap = new org.embulk.guice.Bootstrap()
                .requireExplicitBindings(true)
                .addModules(DigdagEmbed.standardModules());
            moduleOverrides.stream().forEach(override -> bootstrap.overrideModules(override));

            LifeCycleInjector injector;
            if (destroyOnShutdownHook) {
                injector = bootstrap.initialize();
            } else {
                injector = bootstrap.initializeCloseable();
            }

            return new DigdagEmbed(injector);
        }
    }

    private static List<Module> standardModules()
    {
        return Arrays.asList(
                new ObjectMapperModule()
                    .registerModule(new GuavaModule()),
                    //.registerModule(new JodaModule()),
                new DatabaseModule(DatabaseStoreConfig.builder()
                    .type("h2")
                    //.url("jdbc:h2:./test;DB_CLOSE_ON_EXIT=FALSE")
                    .url("jdbc:h2:mem:test;DB_CLOSE_ON_EXIT=FALSE")  // DB should be closed by @PreDestroy otherwise DB could be closed before other @PreDestroy methods that access to the DB
                    .build()),
                (binder) -> {
                    binder.bind(TaskCallbackApi.class).to(InProcessTaskCallbackApi.class).in(Scopes.SINGLETON);
                    binder.bind(ConfigFactory.class).in(Scopes.SINGLETON);
                    binder.bind(ConfigMapper.class).in(Scopes.SINGLETON);
                    binder.bind(DatabaseMigrator.class).in(Scopes.SINGLETON);
                    binder.bind(WorkflowExecutor.class).in(Scopes.SINGLETON);
                    binder.bind(YamlConfigLoader.class).in(Scopes.SINGLETON);
                    binder.bind(TaskQueueDispatcher.class).in(Scopes.SINGLETON);
                    binder.bind(ScheduleStarter.class).in(Scopes.SINGLETON);
                    binder.bind(LocalAgentManager.class).in(Scopes.SINGLETON);
                    binder.bind(SchedulerManager.class).in(Scopes.SINGLETON);
                    binder.bind(LocalSite.class).in(Scopes.SINGLETON);
                    binder.bind(TaskRunnerManager.class).in(Scopes.SINGLETON);
                    binder.bind(TaskQueueManager.class).in(Scopes.SINGLETON);
                    binder.bind(ScheduleExecutor.class).in(Scopes.SINGLETON);
                    binder.bind(SessionMonitorExecutor.class).in(Scopes.SINGLETON);
                    binder.bind(SessionMonitorManager.class).in(Scopes.SINGLETON);
                    binder.bind(WorkflowCompiler.class).in(Scopes.SINGLETON);
                },
                new ExtensionServiceLoaderModule()
        );
    }

    private final LifeCycleInjector injector;

    DigdagEmbed(LifeCycleInjector injector)
    {
        this.injector = injector;
    }

    public Injector getInjector()
    {
        return injector;
    }

    public void destroy() throws Exception
    {
        injector.destroy();
    }
}
