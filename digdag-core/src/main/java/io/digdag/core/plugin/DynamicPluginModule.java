package io.digdag.core.plugin;

import java.nio.file.Paths;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.spi.Plugin;

public class DynamicPluginModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(PluginLoader.class).toProvider(PluginLoaderProvider.class).in(Scopes.SINGLETON);
        binder.bind(DynamicPluginLoader.Builder.class);
    }

    public static class PluginLoaderProvider
            implements Provider<PluginLoader>
    {
        private final PluginLoader pluginLoader;

        @Inject
        public PluginLoaderProvider(Config systemConfig)
        {
            boolean enabled = systemConfig.get("plugin.enabled", boolean.class, true);
            if (enabled) {
                String localRepositoryPath = systemConfig.get("plugin.local-path", String.class, ".digdag/plugins");
                this.pluginLoader = new RemotePluginLoader(Paths.get(localRepositoryPath));
            }
            else {
                this.pluginLoader = new NullPluginLoader();
            }
        }

        @Override
        public PluginLoader get()
        {
            return pluginLoader;
        }
    }
}
