package io.digdag.core.acroute;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.spi.AccountRoutingFactory;

public class AccountRoutingModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(AccountRoutingFactory.class).to(DefaultAccountRoutingFactory.class).in(Scopes.SINGLETON);
    }
}