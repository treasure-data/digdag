package io.digdag.core.database;

import org.skife.jdbi.v2.Handle;

import java.util.function.Supplier;

public interface TransactionManager
{
    /**
     * Return the current transaction object.
     */
    Handle getHandle(ConfigMapper configMapper);

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T> T begin(ThrowableSupplier<T> func)
            throws Exception;

    /**
     * Abort the current transaction and start a new transaction again.
     */
    void reset();

    /**
     * Enable auto commit for testing purpose.
     */
    <T> T autoCommit(ThrowableSupplier<T> func)
            throws Exception;

    @FunctionalInterface
    interface ThrowableSupplier<T>
    {
        T get() throws Exception;
    }
}
