package io.digdag.core.database;

import org.skife.jdbi.v2.Handle;

public interface TransactionManager
{
    /**
     * Return the current transaction object.
     */
    Handle getHandle(ConfigMapper configMapper);

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T> T begin(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException> func);

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception> T begin(
            SupplierInTransaction<T, E1, RuntimeException, RuntimeException> func, Class<E1> e1)
        throws E1;

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception> T begin(
            SupplierInTransaction<T, E1, E2, RuntimeException> func, Class<E1> e1, Class<E2> e2)
        throws E1, E2;

    /**
     * Create a new transaction and set it as the current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception, E3 extends Exception> T begin(
            SupplierInTransaction<T, E1, E2, E3> func, Class<E1> e1, Class<E2> e2, Class<E3> e3)
        throws E1, E2, E3;

    /**
     * Create a new transaction and set it as the current transaction object, or get current transaction object.
     */
    <T> T beginOrReuse(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException> func);

    /**
     * Create a new transaction and set it as the current transaction object, or get current transaction object.
     */
    <T, E1 extends Exception> T beginOrReuse(
            SupplierInTransaction<T, E1, RuntimeException, RuntimeException> func, Class<E1> e1)
        throws E1;

    /**
     * Create a new transaction and set it as the current transaction object, or get current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception> T beginOrReuse(
            SupplierInTransaction<T, E1, E2, RuntimeException> func, Class<E1> e1, Class<E2> e2)
        throws E1, E2;

    /**
     * Create a new transaction and set it as the current transaction object, or get current transaction object.
     */
    <T, E1 extends Exception, E2 extends Exception, E3 extends Exception> T beginOrReuse(
            SupplierInTransaction<T, E1, E2, E3> func, Class<E1> e1, Class<E2> e2, Class<E3> e3)
        throws E1, E2, E3;

    /**
     * Abort the current transaction and start a new transaction again.
     */
    void reset();

    /**
     * Enable auto commit for testing purpose.
     */
    <T> T autoCommit(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException> func);

    /**
     * Enable auto commit for testing purpose.
     */
    <T, E1 extends Exception> T autoCommit(
            SupplierInTransaction<T, E1, RuntimeException, RuntimeException> func, Class<E1> e1)
        throws E1;

    @FunctionalInterface
    interface SupplierInTransaction<T, E1 extends Exception, E2 extends Exception, E3 extends Exception>
    {
        T get() throws E1, E2, E3;
    }
}
