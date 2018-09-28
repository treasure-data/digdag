package io.digdag.standards.operator.param;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.spi.ParamServerClientConnectionManager;

public class ParamServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ParamServerClientConnectionManager.class)
                .toProvider(ParamServerClientConnectionManagerProvider.class).in(Scopes.SINGLETON);
    }
}
