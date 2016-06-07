package io.digdag.core.plugin;

public interface PluginLoader
{
    PluginSetFactory load(Spec spec);
}
