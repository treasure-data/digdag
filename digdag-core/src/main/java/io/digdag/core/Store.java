package io.digdag.core;

public interface Store
{
    <T> T transaction(StoreTransaction<T> transaction);
}
