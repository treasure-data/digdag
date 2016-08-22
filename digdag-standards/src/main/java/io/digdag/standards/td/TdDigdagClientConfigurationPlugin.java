package io.digdag.standards.td;

import com.google.inject.Binder;
import com.treasuredata.client.TDClientConfig;
import io.digdag.spi.DigdagClientConfigurator;
import io.digdag.spi.Plugin;

public class TdDigdagClientConfigurationPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == DigdagClientConfigurator.class) {
            return TdDigdagClientConfigurator.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    @Override
    public <T> void configureBinder(Class<T> type, Binder binder)
    {
        binder.bind(TDClientConfig.class).toProvider(TdClientConfigProvider.class).asEagerSingleton();
    }
}
