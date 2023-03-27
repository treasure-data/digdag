package io.digdag.core.acroute;

import io.digdag.spi.AccountRouting;
import io.digdag.spi.AccountRoutingFactory;

public class DummyAccountRoutingFactory implements AccountRoutingFactory
{
    @Override
    public String getType()
    {
        return "dummy";
    }

    @Override
    public AccountRouting newAccountRouting(AccountRouting.ModuleType module)
    {
        return new AccountRouting()
        {
            @Override
            public Boolean enabled()
            {
                return false;
            }

            @Override
            public String getFilterSQL(String column)
            {
                return "true";
            }
        };
    }
}
