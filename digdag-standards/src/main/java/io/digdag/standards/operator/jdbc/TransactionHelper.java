package io.digdag.standards.operator.jdbc;

import java.util.UUID;

public interface TransactionHelper
{
    interface TransactionAction
    {
        void run();
    }

    void prepare(UUID queryId);

    void cleanup();

    boolean lockedTransaction(UUID queryId, TransactionAction action)
            throws LockConflictException;
}
