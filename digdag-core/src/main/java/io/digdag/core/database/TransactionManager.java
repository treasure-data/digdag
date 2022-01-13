package io.digdag.core.database;

import org.jdbi.v3.core.Handle;

public interface TransactionManager
{
    /**
     * Return the current transaction object.
     */
    Handle getHandle(ConfigMapper configMapper);

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T> T begin(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func);

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception> T begin(
            SupplierInTransaction<T, E1, RuntimeException, RuntimeException, RuntimeException> func, Class<E1> e1)
        throws E1;

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception> T begin(
            SupplierInTransaction<T, E1, E2, RuntimeException, RuntimeException> func, Class<E1> e1, Class<E2> e2)
        throws E1, E2;

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception, E3 extends Exception> T begin(
            SupplierInTransaction<T, E1, E2, E3, RuntimeException> func, Class<E1> e1, Class<E2> e2, Class<E3> e3)
        throws E1, E2, E3;

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception, E3 extends Exception, E4 extends Exception> T begin(
            SupplierInTransaction<T, E1, E2, E3, E4> func, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4)
            throws E1, E2, E3, E4;

    /**
     * Get the current transaction object if exists, otherwise uses a temporary transaction object with auto-commit mode.
     */
    <T> T autoCommit(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func);

    /**
     * Get the current transaction object if exists, otherwise uses a temporary transaction object with auto-commit mode.
     */
    <T, E1 extends Exception> T autoCommit(
            SupplierInTransaction<T, E1, RuntimeException, RuntimeException, RuntimeException> func, Class<E1> e1)
        throws E1;

    /**
     * Get the current transaction object if exists, otherwise uses a temporary transaction object with auto-commit mode.
     */
    <T, E1 extends Exception, E2 extends Exception> T autoCommit(
            SupplierInTransaction<T, E1, E2, RuntimeException, RuntimeException> func, Class<E1> e1, Class<E2> e2)
        throws E1, E2;

    /**
     * Get the current transaction object if exists, otherwise uses a temporary transaction object with auto-commit mode.
     */
    <T, E1 extends Exception, E2 extends Exception, E3 extends Exception> T autoCommit(
            SupplierInTransaction<T, E1, E2, E3, RuntimeException> func, Class<E1> e1, Class<E2> e2, Class<E3> e3)
        throws E1, E2, E3;

    /**
     * Abort the current transaction and start a new transaction again.
     */
    void reset();

    @FunctionalInterface
    interface SupplierInTransaction<T, E1 extends Exception, E2 extends Exception, E3 extends Exception, E4 extends Exception>
    {
        T get()
                throws E1, E2, E3, E4;
    }
}
