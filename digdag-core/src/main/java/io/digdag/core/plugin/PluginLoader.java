package io.digdag.core.plugin;

public interface PluginLoader
{
    PluginSet load(Spec spec);
}
