package io.digdag.standards.td;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.treasuredata.client.TDClientConfig;
import io.digdag.spi.SecretStore;

public class TdConfigurationModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TDClientConfig.class).toProvider(TdClientConfigProvider.class);
        binder.bind(SecretStore.class).annotatedWith(Names.named("td")).to(TdConfigSecretStore.class).asEagerSingleton();
    }
}
