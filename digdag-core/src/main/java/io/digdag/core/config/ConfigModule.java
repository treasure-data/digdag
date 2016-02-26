package io.digdag.core.config;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.digdag.client.config.ConfigFactory;

public class ConfigModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConfigLoaderManager.class).in(Scopes.SINGLETON);
        binder.bind(YamlConfigLoader.class).in(Scopes.SINGLETON);
    }
}
