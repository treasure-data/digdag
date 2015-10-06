package io.digdag.core;

public interface Store  // TODO rename to TransactionalStore
{
    <T> T transaction(StoreTransaction<T> transaction);
}
