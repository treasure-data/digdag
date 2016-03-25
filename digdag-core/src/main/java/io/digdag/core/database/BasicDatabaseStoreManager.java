package io.digdag.core.database;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.TransactionFailedException;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class BasicDatabaseStoreManager <D>
{
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final String databaseType;
    private final Class<D> daoIface;
    private final IDBI dbi;

    protected BasicDatabaseStoreManager(String databaseType, Class<D> daoIface, IDBI dbi)
    {
        this.databaseType = databaseType;
        this.daoIface = daoIface;
        this.dbi = dbi;
    }

    public <T> T requiredResource(T resource, String messageFormat, Object... messageParameters)
            throws ResourceNotFoundException
    {
        if (resource == null) {
            throw new ResourceNotFoundException("Resource does not exist: " + String.format(messageFormat, messageParameters));
        }
        return resource;
    }

    public <T> T requiredResource(AutoCommitAction<T, D> action, String messageFormat, Object... messageParameters)
            throws ResourceNotFoundException
    {
        return requiredResource(autoCommit(action), messageFormat, messageParameters);
    }

    public interface NewResourceAction <T>
    {
        public T call() throws ResourceConflictException;
    }

    public <T> T catchConflict(NewResourceAction<T> function,
            String messageFormat, Object... messageParameters)
            throws ResourceConflictException
    {
        try {
            return function.call();
        }
        catch (UnableToExecuteStatementException ex) {
            if (ex.getCause() instanceof SQLException) {
                SQLException sqlEx = (SQLException) ex.getCause();
                if (isConflictException(sqlEx)) {
                    throw new ResourceConflictException("Resource already exists: " + String.format(messageFormat, messageParameters));
                }
            }
            throw ex;
        }
    }

    public <T> T catchForeignKeyNotFound(NewResourceAction<T> function,
            String messageFormat, Object... messageParameters)
            throws ResourceNotFoundException, ResourceConflictException
    {
        try {
            return function.call();
        }
        catch (UnableToExecuteStatementException ex) {
            if (ex.getCause() instanceof SQLException) {
                SQLException sqlEx = (SQLException) ex.getCause();
                if (isForeignKeyException(sqlEx)) {
                    throw new ResourceNotFoundException("Resource not found: " + String.format(messageFormat, messageParameters));
                }
            }
            throw ex;
        }
    }

    public boolean isConflictException(SQLException ex)
    {
        // h2 and postgresql
        return "23505".equals(ex.getSQLState());
    }

    public boolean isForeignKeyException(SQLException ex)
    {
        switch (databaseType) {
        case "h2":
            return "23506".equals(ex.getSQLState());
        default:
            // TODO postgresql is not checked
            return "23503".equals(ex.getSQLState());
        }
    }

    public interface AutoCommitAction <T, D> {
        T call(Handle handle, D dao);
    }

    public interface AutoCommitActionWithException <T, D, E extends Exception> {
        T call(Handle handle, D dao) throws E;
    }

    public interface AutoCommitActionWithExceptions <T, D, E1 extends Exception, E2 extends Exception> {
        T call(Handle handle, D dao) throws E1, E2;
    }

    public interface TransactionAction <T, D> {
        T call(Handle handle, D dao, TransactionState ts);
    }

    public interface TransactionActionWithException <T, D, E extends Exception> {
        T call(Handle handle, D dao, TransactionState ts) throws E;
    }

    public interface TransactionActionWithExceptions <T, D, E1 extends Exception, E2 extends Exception> {
        T call(Handle handle, D dao, TransactionState ts) throws E1, E2;
    }

    public static class TransactionState {
        private boolean retryNext = false;
        private int retryCount = 0;
        private Throwable lastException;

        public boolean isRetried()
        {
            return retryCount > 0;
        }

        public Throwable getLastException()
        {
            return lastException;
        }

        public void retry()
        {
            retryNext = true;
            lastException = null;
        }

        public void retry(Throwable exception)
        {
            retryNext = true;
            lastException = exception;
        }
    }

    private static class InnerException
            extends RuntimeException
    {
        private final int index;

        public InnerException(Throwable cause)
        {
            this(cause, 0);
        }

        public InnerException(Throwable cause, int index)
        {
            super(cause);
            this.index = index;
        }

        public int getIndex()
        {
            return index;
        }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public <T> T transaction(TransactionAction<T, D> action)
    {
        TransactionState ts = new TransactionState();
        while (true) {
            try (Handle handle = dbi.open()) {
                T retval = handle.inTransaction((h, status) -> action.call(h, h.attach(daoIface), ts));
                if (ts.retryNext) {
                    ts.retryNext = false;
                    ts.retryCount += 1;
                }
                else {
                    return retval;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E extends Exception> T transaction(TransactionActionWithException<T, D, E> action, Class<E> exClass) throws E
    {
        TransactionState ts = new TransactionState();
        while (true) {
            try (Handle handle = dbi.open()) {
                T retval;
                // Here doesn't use handle.inTransaction See comments on validateTransactionAndCommit.
                handle.begin();
                try {
                    retval = action.call(handle, handle.attach(daoIface), ts);
                }
                catch (Exception ex) {
                    try {
                        handle.rollback();
                    }
                    catch (Exception rollback) {
                        ex.addSuppressed(rollback);
                    }
                    Throwables.propagateIfInstanceOf(ex, exClass);
                    Throwables.propagateIfPossible(ex);
                    throw new TransactionFailedException(
                            "Transaction failed do to exception being thrown " +
                            "from within the callback. See cause " +
                            "for the original exception.", ex);
                }
                if (!ts.retryNext) {
                    validateTransactionAndCommit(handle);
                    return retval;
                }
                handle.rollback();
                ts.retryNext = false;
                ts.retryCount += 1;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E1 extends Exception, E2 extends Exception> T transaction(TransactionActionWithExceptions<T, D, E1, E2> action, Class<E1> exClass1, Class<E2> exClass2) throws E1, E2
    {
        TransactionState ts = new TransactionState();
        while (true) {
            try (Handle handle = dbi.open()) {
                T retval;
                // Here doesn't use handle.inTransaction See comments on validateTransactionAndCommit.
                handle.begin();
                try {
                    retval = action.call(handle, handle.attach(daoIface), ts);
                }
                catch (Exception ex) {
                    try {
                        handle.rollback();
                    }
                    catch (Exception rollback) {
                        ex.addSuppressed(rollback);
                    }
                    Throwables.propagateIfInstanceOf(ex, exClass1);
                    Throwables.propagateIfInstanceOf(ex, exClass2);
                    Throwables.propagateIfPossible(ex);
                    throw new TransactionFailedException(
                            "Transaction failed do to exception being thrown " +
                            "from within the callback. See cause " +
                            "for the original exception.", ex);
                }
                if (!ts.retryNext) {
                    validateTransactionAndCommit(handle);
                    return retval;
                }
                ts.retryNext = false;
                ts.retryCount += 1;
            }
        }
    }

    private void validateTransactionAndCommit(Handle handle)
        throws TransactionFailedException
    {
        // Validate connection before COMMIT. This is necessary because PostgreSQL actually runs
        // ROLLBACK silently when COMMIT is issued if a statement failed during the transaction.
        // It means that BasicDatabaseStoreManager.transaction method doesn't throw exceptions
        // but the actual transaction is rolled back. Here checks state of the transaction by
        // calling org.postgresql.jdbc.PgConnection.isValid method.
        //
        // Here assumes that HikariDB doesn't overwrite isValid.
        boolean isValid;
        try {
            isValid = handle.getConnection().isValid(30);
        }
        catch (SQLException ex) {
            throw new TransactionFailedException(
                    "Can't validate a transaction before commit", ex);
        }
        if (!isValid) {
            throw new TransactionFailedException(
                    "Trying to commit a transaction that is already aborted. "  +
                    "Because current transaction is aborted, commands including " +
                    "commit are ignored until end of transaction block.");
        }
        else {
            handle.commit();
        }
    }

    public <T> T autoCommit(AutoCommitAction<T, D> action)
    {
        try (Handle handle = dbi.open()) {
            return action.call(handle, handle.attach(daoIface));
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E extends Exception> T autoCommit(AutoCommitActionWithException<T, D, E> action, Class<E> exClass) throws E
    {
        try (Handle handle = dbi.open()) {
            return action.call(handle, handle.attach(daoIface));
        }
        catch (InnerException ex) {
            throw (E) ex.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E1 extends Exception, E2 extends Exception> T autoCommit(AutoCommitActionWithExceptions<T, D, E1, E2> action, Class<E1> exClass1, Class<E2> exClass2) throws E1, E2
    {
        try (Handle handle = dbi.open()) {
            T retval = handle.inTransaction((h, status) -> {
                try {
                    return action.call(h, h.attach(daoIface));
                }
                catch (Exception ex) {
                    if (exClass1.isAssignableFrom(ex.getClass())) {
                        throw new InnerException(ex, 1);
                    }
                    else if (exClass2.isAssignableFrom(ex.getClass())) {
                        throw new InnerException(ex, 2);
                    }
                    throw ex;
                }
            });
            return retval;
        }
        catch (InnerException ex) {
            if (ex.getIndex() == 1) {
                throw (E1) ex.getCause();
            }
            else {
                throw (E2) ex.getCause();
            }
        }
    }

    public static Optional<Integer> getOptionalInt(ResultSet r, String column)
            throws SQLException
    {
        int v = r.getInt(column);
        return optional(r.wasNull(), v);
    }

    public static Optional<Long> getOptionalLong(ResultSet r, String column)
            throws SQLException
    {
        long v = r.getLong(column);
        return optional(r.wasNull(), v);
    }

    public static UUID getUuid(ResultSet r, String column)
            throws SQLException
    {
        String v = r.getString(column);
        return UUID.fromString(v);
    }

    public static Instant getTimestampInstant(ResultSet r, String column)
            throws SQLException
    {
        return r.getTimestamp(column).toInstant();
    }

    public static Optional<Instant> getOptionalTimestampInstant(ResultSet r, String column)
            throws SQLException
    {
        Timestamp t = r.getTimestamp(column);
        if (r.wasNull()) {
            return Optional.absent();
        }
        else {
            return Optional.of(t.toInstant());
        }
    }

    public static Optional<String> getOptionalString(ResultSet r, String column)
            throws SQLException
    {
        String v = r.getString(column);
        return optional(r.wasNull(), v);
    }

    public static Optional<byte[]> getOptionalBytes(ResultSet r, String column)
            throws SQLException
    {
        byte[] v = r.getBytes(column);
        return optional(r.wasNull(), v);
    }

    public static List<Long> getLongIdList(ResultSet r, String column)
            throws SQLException
    {
        String v = r.getString(column);
        if (r.wasNull()) {
            return new ArrayList<>();
        }
        return Stream.of(v.split(","))
            .map(it -> Long.parseLong(it))
            .collect(Collectors.toList());
    }

    private static <T> Optional<T> optional(boolean wasNull, T v)
    {
        if (wasNull) {
            return Optional.absent();
        }
        else {
            return Optional.of(v);
        }
    }
}
