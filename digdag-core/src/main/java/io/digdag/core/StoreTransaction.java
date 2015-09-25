package io.digdag.core;

public interface StoreTransaction <T>
{
    T call();
}
