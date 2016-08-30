package io.digdag.core.plugin;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.digdag.spi.Plugin;

import java.util.List;

public class PluginSet
{
    public static PluginSet empty()
    {
        return new PluginSet(ImmutableList.of());
    }

    public class WithInjector
    {
        private final Injector injector;

        private WithInjector(Injector injector)
        {
            this.injector = injector;
        }

        public <T> List<T> getServiceProviders(Class<T> type)
        {
            ImmutableList.Builder<T> builder = ImmutableList.builder();
            for (Plugin plugin : plugins) {
                Class<? extends T> providerClass = plugin.getServiceProvider(type);
                if (providerClass != null) {
                    builder.add(getServiceProvider(providerClass, type, plugin));
                }
            }
            return builder.build();
        }

        private <T, E extends T> E getServiceProvider(Class<E> providerClass, Class<T> type, Plugin plugin)
        {
            return injector.createChildInjector((binder) -> {
                plugin.configureBinder(providerClass, binder);
                binder.bind(providerClass);
            })
            .getInstance(providerClass);
        }
    }

    private final List<Plugin> plugins;

    public PluginSet(List<Plugin> plugins)
    {
        this.plugins = plugins;
    }

    public PluginSet withPlugins(Plugin... plugins)
    {
        return withPlugins(ImmutableList.copyOf(plugins));
    }

    public PluginSet withPlugins(List<Plugin> plugins)
    {
        return new PluginSet(FluentIterable
                .from(this.plugins)
                .append(plugins)
                .toList());
    }

    public PluginSet withPlugins(PluginSet another)
    {
        return withPlugins(another.plugins);
    }

    public WithInjector withInjector(Injector injector)
    {
        return new WithInjector(injector);
    }
}
