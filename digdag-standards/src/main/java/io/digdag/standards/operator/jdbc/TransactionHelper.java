package io.digdag.standards.operator.jdbc;

import java.util.UUID;
import com.google.common.base.Optional;

public interface TransactionHelper
{
    interface TransactionAction <T>
    {
        T run();
    }

    void prepare();

    void cleanup();

    <T> Optional<T> lockedTransaction(UUID queryId, TransactionAction<T> action);
}
