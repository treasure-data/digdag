package io.digdag.core.plugin;

import java.util.List;
import com.google.inject.Module;
import com.google.inject.Binder;
import io.digdag.spi.Plugin;

public class SystemPluginModule
        implements Module
{
    private final PluginFactorySet factories;

    public SystemPluginModule(PluginFactorySet factories)
    {
        this.factories = factories;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(PluginFactorySet.class).toInstance(factories);
        binder.bind(PluginSet.class).toProvider(PluginSetProvider.class);
    }
}
