package io.digdag.core.plugin;

import java.util.List;
import java.util.Collection;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.digdag.spi.Plugin;
import io.digdag.spi.PluginFactory;

public class PluginSetFactory
{
    public static PluginSetFactory empty()
    {
        return new PluginSetFactory(ImmutableList.of());
    }

    private final List<PluginFactory> plugins;

    public PluginSetFactory(Collection<? extends PluginFactory> plugins)
    {
        this.plugins = ImmutableList.copyOf(plugins);
    }

    public PluginSet create(Injector injector)
    {
        ImmutableList.Builder<Plugin> builder = ImmutableList.builder();
        for (PluginFactory factory : plugins) {
            builder.add(factory.create(injector));
        }
        return new PluginSet(builder.build());
    }
}
