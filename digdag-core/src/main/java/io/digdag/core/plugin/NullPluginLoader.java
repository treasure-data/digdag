package io.digdag.core.plugin;

public class NullPluginLoader
        implements PluginLoader
{
    public PluginSetFactory load(Spec spec)
    {
        return PluginSetFactory.empty();
    }
}
