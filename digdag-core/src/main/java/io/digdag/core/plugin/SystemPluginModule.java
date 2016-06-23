package io.digdag.core.plugin;

import com.google.inject.Module;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Provider;
import io.digdag.spi.Plugin;

public class SystemPluginModule
        implements Module
{
    private final PluginSet plugins;

    public SystemPluginModule(PluginSet plugins)
    {
        this.plugins = plugins;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(PluginSet.class).toInstance(plugins);
        binder.bind(PluginSet.WithInjector.class).toProvider(PluginSetWithInjectorProvider.class).in(Scopes.SINGLETON);
    }

    private static class PluginSetWithInjectorProvider
            implements Provider<PluginSet.WithInjector>
    {
        private PluginSet.WithInjector instance;

        @Inject
        public PluginSetWithInjectorProvider(PluginSet pluginSet, Injector injector)
        {
            this.instance = pluginSet.withInjector(injector);
        }

        @Override
        public PluginSet.WithInjector get()
        {
            return instance;
        }
    }
}
