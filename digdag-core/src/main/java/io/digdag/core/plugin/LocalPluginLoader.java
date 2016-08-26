package io.digdag.core.plugin;

import com.google.common.collect.ImmutableList;

import io.digdag.spi.Plugin;

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceConfigurationError;

public class LocalPluginLoader
{
    public LocalPluginLoader()
    { }

    public PluginSet load(ClassLoader classLoader)
    {
        try {
            return new PluginSet(lookupPlugins(classLoader));
        }
        catch (ServiceConfigurationError ex) {
            throw new RuntimeException("Failed to lookup io.digdag.spi.Plugin service from local class loader " + classLoader, ex);
        }
    }

    static List<Plugin> lookupPlugins(ClassLoader classLoader)
        throws ServiceConfigurationError
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, classLoader);
        return ImmutableList.copyOf(serviceLoader);
    }
}
