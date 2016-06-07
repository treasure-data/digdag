package io.digdag.spi;

import com.google.inject.Injector;

public interface PluginFactory
{
    Plugin create(Injector injector);
}
