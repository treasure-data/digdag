package io.digdag.standards.operator.jdbc;

import java.util.UUID;

public class NoTransactionHelper
    implements TransactionHelper
{
    @Override
    public void prepare()
    { }

    @Override
    public void cleanup()
    { }

    @Override
    public boolean lockedTransaction(UUID queryId, TransactionAction action)
    {
        action.run();
        return true;
    }
}
