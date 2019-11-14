package io.digdag.standards.td;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.treasuredata.client.TDClientConfig;
import io.digdag.spi.SecretStore;
import io.digdag.standards.operator.td.BaseTDClientFactory;
import io.digdag.standards.operator.td.TDClientFactory;

public class TdConfigurationModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TDClientConfig.class).toProvider(TdClientConfigProvider.class);
        binder.bind(BaseTDClientFactory.class).to(TDClientFactory.class).asEagerSingleton();
        Multibinder.newSetBinder(binder, SecretStore.class).addBinding().to(TdConfigSecretStore.class).asEagerSingleton();
    }
}
