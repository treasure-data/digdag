package io.digdag.core.plugin;

import java.util.List;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.Plugin;

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
                    builder.add(getServiceProvider(providerClass, type));
                }
            }
            return builder.build();
        }

        private <T, E extends T> E getServiceProvider(Class<E> providerClass, Class<T> type)
        {
            return injector.createChildInjector((binder) -> {
                binder.<E>bind(providerClass);
            })
            .getInstance(providerClass);
        }
    }

    private final List<Plugin> plugins;

    public PluginSet(List<Plugin> plugins)
    {
        this.plugins = plugins;
    }

    public WithInjector withInjector(Injector injector)
    {
        return new WithInjector(injector);
    }
}
