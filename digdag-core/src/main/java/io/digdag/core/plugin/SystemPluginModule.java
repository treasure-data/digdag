package io.digdag.core.plugin;

import com.google.inject.Module;
import com.google.inject.Binder;
import io.digdag.spi.Plugin;

public class SystemPluginModule
        implements Module
{
    private final PluginSetFactory factory;

    public SystemPluginModule(PluginSetFactory factory)
    {
        this.factory = factory;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(PluginSetFactory.class).toInstance(factory);
        binder.bind(PluginSet.class).toProvider(PluginSetProvider.class);
    }
}
