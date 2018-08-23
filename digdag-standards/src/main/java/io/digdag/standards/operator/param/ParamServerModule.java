package io.digdag.standards.operator.param;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class ParamServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ParamServerClientConnectionManager.class)
                .toProvider(ParamServerClientConnectionManagerProvider.class).in(Scopes.SINGLETON);
        binder.bind(ParamServerClientConnection.class)
                .toProvider(ParamServerClientConnectionProvider.class); // do not make this class singleton because this object is fetched from multiple threads
        binder.bind(ParamServerClient.class)
                .toProvider(ParamServerClientProvider.class); // do not make this class singleton because this object is fetched from multiple threads
    }
}
