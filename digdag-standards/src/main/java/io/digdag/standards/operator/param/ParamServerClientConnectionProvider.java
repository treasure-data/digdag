package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.Provider;

public class ParamServerClientConnectionProvider
        implements Provider<ParamServerClientConnection>
{
    private final ParamServerClientConnectionManager manager;

    @Inject
    public ParamServerClientConnectionProvider(ParamServerClientConnectionManager manager)
    {
        this.manager = manager;
    }

    @Override
    public ParamServerClientConnection get()
    {
        return this.manager.getConnection();
    }
}
