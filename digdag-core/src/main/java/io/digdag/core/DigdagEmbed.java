package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Guice;
import com.google.inject.Provider;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import com.google.inject.multibindings.Multibinder;
import io.digdag.core.queue.QueueModule;
import io.digdag.core.log.NullLogServerFactory;
import io.digdag.core.log.LocalFileLogServerFactory;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.database.DatabaseModule;
import io.digdag.core.workflow.WorkflowModule;
import io.digdag.core.workflow.WorkflowExecutorModule;
import io.digdag.core.schedule.ScheduleModule;
import io.digdag.core.schedule.ScheduleExecutorModule;
import io.digdag.core.config.ConfigModule;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.agent.AgentModule;
import io.digdag.core.agent.LocalAgentModule;
import io.digdag.core.log.LogModule;
import io.digdag.core.plugin.PluginSetFactory;
import io.digdag.core.plugin.DynamicPluginModule;
import io.digdag.core.plugin.SystemPluginModule;
import org.embulk.guice.LifeCycleInjector;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.JacksonTimeModule;

public class DigdagEmbed
{
    public static class Bootstrap
    {
        private final List<Function<? super List<Module>, ? extends Iterable<? extends Module>>> moduleOverrides = new ArrayList<>();
        private ConfigElement systemConfig = ConfigElement.empty();
        private PluginSetFactory systemPlugins = PluginSetFactory.empty();
        private boolean withWorkflowExecutor = true;
        private boolean withScheduleExecutor = true;
        private boolean withLocalAgent = true;
        private boolean withExtensionLoader = true;

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

        public Bootstrap overrideModulesWith(Module... overridingModules)
        {
            return overrideModulesWith(Arrays.asList(overridingModules));
        }

        public Bootstrap overrideModulesWith(Iterable<? extends Module> overridingModules)
        {
            return overrideModules(modules -> ImmutableList.of(Modules.override(modules).with(overridingModules)));
        }

        public Bootstrap setSystemConfig(ConfigElement systemConfig)
        {
            this.systemConfig = systemConfig;
            return this;
        }

        public Bootstrap setSystemPlugins(PluginSetFactory systemPlugins)
        {
            this.systemPlugins = systemPlugins;
            return this;
        }

        public Bootstrap withWorkflowExecutor(boolean v)
        {
            this.withWorkflowExecutor = v;
            return this;
        }

        public Bootstrap withScheduleExecutor(boolean v)
        {
            this.withScheduleExecutor = v;
            return this;
        }

        public Bootstrap withLocalAgent(boolean v)
        {
            this.withLocalAgent = v;
            return this;
        }

        public Bootstrap withExtensionLoader(boolean v)
        {
            this.withExtensionLoader = v;
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
                .addModules(standardModules(systemConfig));
            moduleOverrides.stream().forEach(override -> bootstrap.overrideModules(override));

            LifeCycleInjector injector;
            if (destroyOnShutdownHook) {
                injector = bootstrap.initialize();
            }
            else {
                injector = bootstrap.initializeCloseable();
            }

            return new DigdagEmbed(injector);
        }

        private List<Module> standardModules(ConfigElement systemConfig)
        {
            ImmutableList.Builder<Module> builder = ImmutableList.builder();
            builder.addAll(Arrays.asList(
                    new ObjectMapperModule()
                        .registerModule(new GuavaModule())
                        .registerModule(new JacksonTimeModule()),
                    new DynamicPluginModule(),
                    new SystemPluginModule(systemPlugins),
                    new DatabaseModule(),
                    new AgentModule(),
                    new LogModule(),
                    new ScheduleModule(),
                    new ConfigModule(),
                    new WorkflowModule(),
                    new QueueModule(),
                    (binder) -> {
                        binder.bind(ProjectArchiveLoader.class);
                        binder.bind(ConfigElement.class).toInstance(systemConfig);
                        binder.bind(Config.class).toProvider(SystemConfigProvider.class);
                        binder.bind(TempFileManager.class).toProvider(TempFileManagerProvider.class).in(Scopes.SINGLETON);
                    }
                ));
            if (withWorkflowExecutor) {
                builder.add(new WorkflowExecutorModule());
            }
            if (withScheduleExecutor) {
                builder.add(new ScheduleExecutorModule());
            }
            if (withLocalAgent) {
                builder.add(new LocalAgentModule());
            }
            if (withWorkflowExecutor) {
                builder.add((binder) -> {
                    binder.bind(LocalSite.class).in(Scopes.SINGLETON);
                });
            }
            if (withExtensionLoader) {
                builder.add(new ExtensionServiceLoaderModule());
            }
            return builder.build();
        }
    }

    public static class SystemConfigProvider
            implements Provider<Config>
    {
        private Config systemConfig;

        @Inject
        public SystemConfigProvider(ConfigElement ce, ConfigFactory cf)
        {
            this.systemConfig = ce.toConfig(cf);
        }

        @Override
        public Config get()
        {
            return systemConfig;
        }
    }

    public static class TempFileManagerProvider
            implements Provider<TempFileManager>
    {
        private final TempFileManager manager;

        @Inject
        public TempFileManagerProvider(Config systemConfig)
        {
            Path dir = systemConfig.getOptional("tmpdir", String.class)
                    .transform(s -> Paths.get(s))
                    .or(() -> {
                        try {
                            return Files.createTempDirectory("digdag-tempdir");
                        }
                        catch (IOException ex) {
                            throw Throwables.propagate(ex);
                        }
                    });
            this.manager = new TempFileManager(dir);
        }

        @Override
        public TempFileManager get()
        {
            return manager;
        }
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

    public LocalSite getLocalSite()
    {
        return getInjector().getInstance(LocalSite.class);
    }

    public void destroy() throws Exception
    {
        injector.destroy();
    }
}
