package io.digdag.core.database;

import com.google.inject.Inject;
import io.digdag.commons.ThrowablesUtil;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

public class ThreadLocalTransactionManager
        implements TransactionManager
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    private final ThreadLocal<Transaction> threadLocalTransaction = new ThreadLocal<>();
    private final ThreadLocal<Transaction> threadLocalAutoCommitTransaction = new ThreadLocal<>();
    private final DataSource ds;

    private static class LazyTransaction
            implements Transaction
    {
        private enum State
        {
            ACTIVE,
            ABORTED,
            COMMITTED;
        }

        private final DataSource ds;
        private final boolean autoAutoCommit;
        private Handle handle;
        private State state = State.ACTIVE;
        private final StackTraceElement[] stackTrace;

        LazyTransaction(DataSource ds)
        {
            this(ds, false);
        }

        LazyTransaction(DataSource ds, boolean autoAutoCommit)
        {
            this.ds = checkNotNull(ds);
            this.autoAutoCommit = autoAutoCommit;
            this.stackTrace = Thread.currentThread().getStackTrace();
        }

        @Override
        public Handle getHandle(ConfigMapper configMapper)
        {
            if (state != State.ACTIVE) {
                throw new IllegalStateException("Transaction is already " + state.name().toLowerCase(ENGLISH));
            }

            if (handle == null) {
                Jdbi dbi = Jdbi.create(ds);
                ConfigKeyListMapper cklm = new ConfigKeyListMapper();
                dbi.registerRowMapper(new DatabaseProjectStoreManager.StoredProjectMapper(configMapper));
                dbi.registerRowMapper(new DatabaseProjectStoreManager.StoredProjectWithRevisionMapper(configMapper));
                dbi.registerRowMapper(new DatabaseProjectStoreManager.StoredRevisionMapper(configMapper));
                dbi.registerRowMapper(new DatabaseProjectStoreManager.StoredWorkflowDefinitionMapper(configMapper));
                dbi.registerRowMapper(new DatabaseProjectStoreManager.StoredWorkflowDefinitionWithProjectMapper(configMapper));
                dbi.registerRowMapper(new DatabaseProjectStoreManager.WorkflowConfigMapper());
                dbi.registerRowMapper(new DatabaseProjectStoreManager.IdNameMapper());
                dbi.registerRowMapper(new DatabaseProjectStoreManager.ScheduleStatusMapper());
                dbi.registerRowMapper(new DatabaseQueueSettingStoreManager.StoredQueueSettingMapper(configMapper));
                dbi.registerRowMapper(new DatabaseScheduleStoreManager.StoredScheduleMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredTaskMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.ArchivedTaskMapper(cklm, configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.ResumingTaskMapper(cklm, configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredSessionMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredSessionWithLastAttemptMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredSessionAttemptMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredSessionAttemptWithSessionMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.TaskStateSummaryMapper());
                dbi.registerRowMapper(new DatabaseSessionStoreManager.TaskAttemptSummaryMapper());
                dbi.registerRowMapper(new DatabaseSessionStoreManager.SessionAttemptSummaryMapper());
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredSessionMonitorMapper(configMapper));
                dbi.registerRowMapper(new DatabaseSessionStoreManager.StoredDelayedSessionAttemptMapper());
                dbi.registerRowMapper(new DatabaseSessionStoreManager.TaskRelationMapper());
                dbi.registerRowMapper(new DatabaseSessionStoreManager.InstantMapper());
                dbi.registerRowMapper(new DatabaseSecretStore.ScopedSecretMapper());
                dbi.registerRowMapper(new DatabaseTaskQueueServer.ImmutableTaskQueueLockMapper());
                dbi.registerArgument(configMapper.getArgumentFactory());
                handle = dbi.open();

                try {
                    handle.getConnection().setAutoCommit(autoAutoCommit);
                }
                catch (SQLException ex) {
                    throw new TransactionException("Failed to set auto commit: " + autoAutoCommit, ex);
                }
                if (!autoAutoCommit) {
                    handle.begin();
                }
            }
            return handle;
        }

        @Override
        public void commit()
        {
            if (handle == null) {
                return;
            }
            if (state != State.ACTIVE) {
                throw new IllegalStateException("Committing " + state.name().toLowerCase(ENGLISH) + " is not allowed");
            }

            // Validate connection before COMMIT. This is necessary because PostgreSQL actually runs
            // ROLLBACK silently when COMMIT is issued if a statement failed during the transaction.
            // It means that BasicDatabaseStoreManager.transaction method doesn't throw exceptions
            // but the actual transaction is rolled back. Here checks state of the transaction by
            // calling org.postgresql.jdbc.PgConnection.isValid method.
            //
            // Here assumes that HikariDB doesn't override isValid.
            boolean isValid;
            try {
                isValid = handle.getConnection().isValid(30);
            }
            catch (SQLException ex) {
                throw new TransactionException(
                        "Can't validate a transaction before commit", ex);
            }
            if (!isValid) {
                throw new TransactionException(
                        "Trying to commit a transaction that is already aborted. " +
                                "Because current transaction is aborted, commands including " +
                                "commit are ignored until end of transaction block.");
            }
            else {
                handle.commit();
            }

            state = State.COMMITTED;
        }

        @Override
        public void abort()
        {
            if (handle == null) {
                return;
            }
            if (state == State.COMMITTED) {
                throw new IllegalStateException("Aborting committed transaction is not allowed");
            }
            if (!autoAutoCommit) {
                handle.rollback();
            }
            state = State.ABORTED;
        }

        @Override
        public void reset()
        {
            abort();
            state = State.ACTIVE;
        }

        void close()
        {
            if (handle != null) {
                handle.close();
            }
        }

        @Override
        public String toString()
        {
            return "LazyTransaction{" +
                    "autoAutoCommit=" + autoAutoCommit +
                    ", handle=" + handle +
                    ", state=" + state +
                    ", stackTrace=[\n" + Arrays.stream(stackTrace)
                            .map(st -> "  " + st.toString())
                            .collect(Collectors.joining("\n")) +
                    "\n]}";
        }
    }

    @Inject
    public ThreadLocalTransactionManager(DataSource ds)
    {
        this(ds, false);
    }

    ThreadLocalTransactionManager(DataSource ds, boolean autoAutoCommit)
    {
        this.ds = checkNotNull(ds);
        if (autoAutoCommit) {
            LazyTransaction transaction = new LazyTransaction(ds, true);
            threadLocalTransaction.set(transaction);
        }
    }

    @Override
    public Handle getHandle(ConfigMapper configMapper)
    {
        Transaction transaction = threadLocalTransaction.get();
        if (transaction == null) {
            transaction = threadLocalAutoCommitTransaction.get();
            if (transaction == null) {
		// TODO
		// refactoring and make source code simple.
		// better to create ready-only-transaction to remove explicit begin() call.
                throw new IllegalStateException("Not in transaction");
            }
        }
        return transaction.getHandle(configMapper);
    }

    @Override
    public <T> T begin(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func)
    {
        return begin(func, RuntimeException.class, RuntimeException.class, RuntimeException.class, RuntimeException.class);
    }

    @Override
    public <T, E1 extends Exception> T begin(SupplierInTransaction<T, E1, RuntimeException, RuntimeException, RuntimeException> func, Class<E1> e1)
            throws E1
    {
        return begin(func, e1, RuntimeException.class, RuntimeException.class, RuntimeException.class);
    }

    @Override
    public <T, E1 extends Exception, E2 extends Exception>
    T begin(SupplierInTransaction<T, E1, E2, RuntimeException, RuntimeException> func, Class<E1> e1, Class<E2> e2)
            throws E1, E2
    {
        return begin(func, e1, e2, RuntimeException.class, RuntimeException.class);
    }

    @Override
    public <T, E1 extends Exception, E2 extends Exception, E3 extends Exception>
    T begin(SupplierInTransaction<T, E1, E2, E3, RuntimeException> func, Class<E1> e1, Class<E2> e2, Class<E3> e3)
            throws E1, E2, E3
    {
        return begin(func, e1, e2, e3, RuntimeException.class);
    }

    public <T, E1 extends Exception, E2 extends Exception, E3 extends Exception, E4 extends Exception>
    T begin(SupplierInTransaction<T, E1, E2, E3, E4> func, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4)
            throws E1, E2, E3, E4
    {
        if (threadLocalTransaction.get() != null) {
            throw new IllegalStateException("Nested transaction is not allowed: " + threadLocalTransaction.get());
        }

        boolean committed = false;
        LazyTransaction transaction = new LazyTransaction(ds);
        try {
            threadLocalTransaction.set(transaction);
            T result = func.get();
            transaction.commit();
            committed = true;
            return result;
        }
        catch (Exception e) {
            ThrowablesUtil.propagateIfInstanceOf(e, e1);
            ThrowablesUtil.propagateIfInstanceOf(e, e2);
            ThrowablesUtil.propagateIfInstanceOf(e, e3);
            ThrowablesUtil.propagateIfInstanceOf(e, e4);
            throw ThrowablesUtil.propagate(e);
        }
        finally {
            threadLocalTransaction.set(null);
            try {
                if (!committed) {
                    transaction.abort();
                }
            }
            finally {
                transaction.close();
            }
        }
    }

    @Override
    public <T> T autoCommit(SupplierInTransaction<T, RuntimeException, RuntimeException, RuntimeException, RuntimeException> func)
    {
        return autoCommit(func, RuntimeException.class, RuntimeException.class, RuntimeException.class);
    }

    @Override
    public <T, E1 extends Exception> T autoCommit(SupplierInTransaction<T, E1, RuntimeException, RuntimeException, RuntimeException> func, Class<E1> e1)
            throws E1
    {
        return autoCommit(func, e1, RuntimeException.class, RuntimeException.class);
    }

    @Override
    public <T, E1 extends Exception, E2 extends Exception>
    T autoCommit(SupplierInTransaction<T, E1, E2, RuntimeException, RuntimeException> func, Class<E1> e1, Class<E2> e2)
            throws E1, E2
    {
        return autoCommit(func, e1, e2, RuntimeException.class);
    }

    @Override
    public <T, E1 extends Exception, E2 extends Exception, E3 extends Exception>
    T autoCommit(SupplierInTransaction<T, E1, E2, E3, RuntimeException> func, Class<E1> e1, Class<E2> e2, Class<E3> e3)
            throws E1, E2, E3
    {
        try {
            if (threadLocalTransaction.get() != null) {
                return func.get();
            }
            else {
                LazyTransaction transaction = new LazyTransaction(ds, true);
                threadLocalAutoCommitTransaction.set(transaction);
                try {
                    return func.get();
                }
                finally {
                    threadLocalAutoCommitTransaction.set(null);
                    transaction.close();
                }
            }
        }
        catch (Exception e) {
            ThrowablesUtil.propagateIfInstanceOf(e, e1);
            ThrowablesUtil.propagateIfInstanceOf(e, e2);
            ThrowablesUtil.propagateIfInstanceOf(e, e3);
            throw ThrowablesUtil.propagate(e);
        }
    }

    @Override
    public void reset()
    {
        Transaction transaction = threadLocalTransaction.get();
        if (transaction == null) {
            throw new IllegalStateException("Not in transaction");
        }
        transaction.reset();
    }
}
