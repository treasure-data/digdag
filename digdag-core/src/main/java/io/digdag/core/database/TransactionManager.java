package io.digdag.core.database;

import org.skife.jdbi.v2.Handle;

import java.util.function.Supplier;

public interface TransactionManager
{
    Handle getHandle(ConfigMapper configMapper);

    <T> T begin(ThrowableSupplier<T> func)
            throws Exception;

    @FunctionalInterface
    interface ThrowableSupplier<T>
    {
        T get() throws Exception;
    }
}
