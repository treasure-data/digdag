package io.digdag.core.plugin;

import java.util.List;
import java.util.Collection;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.Plugin;

public class PluginSet
{
    private final List<Plugin> plugins;

    public PluginSet(Collection<? extends Plugin> plugins)
    {
        this.plugins = ImmutableList.copyOf(plugins);
    }

    public <T> List<T> get(Class<T> iface)
    {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (Plugin plugin : plugins) {
            builder.addAll(plugin.get(iface));
        }
        return builder.build();
    }
}
