package io.digdag.core;

import io.digdag.core.StoreTransaction;

public interface Store  // TODO rename to TransactionalStore
{
    <T> T transaction(StoreTransaction<T> transaction);
}
