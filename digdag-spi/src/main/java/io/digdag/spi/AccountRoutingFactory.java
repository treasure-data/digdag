package io.digdag.spi;

import static io.digdag.spi.AccountRouting.ModuleType;

public interface AccountRoutingFactory
{
    String getType();

    AccountRouting newAccountRouting(ModuleType module);
}
