package io.digdag.core.plugin;

import com.google.inject.Guice;
import com.google.inject.Module;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Stage;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TemplateEngine;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class PluginSetProvider
        implements Provider<PluginSet>
{
    private final PluginSet plugins;

    @Inject
    public PluginSetProvider(
            PluginSetFactory factory,
            CommandExecutor commandExecutor,
            TemplateEngine templateEngine,
            ConfigFactory cf,
            Config systemConfig)
    {
        Module mod = (binder) -> {
            binder.bind(CommandExecutor.class).toInstance(commandExecutor);
            binder.bind(TemplateEngine.class).toInstance(templateEngine);
            binder.bind(ConfigFactory.class).toInstance(cf);
            binder.bind(Config.class).toInstance(systemConfig);
        };
        Injector injector = Guice.createInjector(
                Stage.PRODUCTION,
                ImmutableList.of(mod));
        this.plugins = factory.create(injector);
    }

    @Override
    public PluginSet get()
    {
        return plugins;
    }
}
