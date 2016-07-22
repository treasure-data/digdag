package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;

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
    public <T> Optional<T> lockedTransaction(TransactionAction<T> action)
    {
        return Optional.of(action.run());
    }
}
