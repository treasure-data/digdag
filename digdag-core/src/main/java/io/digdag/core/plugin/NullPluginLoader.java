package io.digdag.core.plugin;

public class NullPluginLoader
        implements PluginLoader
{
    public PluginSet load(Spec spec)
    {
        return PluginSet.empty();
    }
}
