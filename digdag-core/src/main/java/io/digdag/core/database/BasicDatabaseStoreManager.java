package io.digdag.core.database;

import java.util.List;
import java.util.ArrayList;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import com.google.common.base.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;

public abstract class BasicDatabaseStoreManager <D>
{
    public static interface HandleFactory
    {
        Handle open();
    }

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final String databaseType;
    private final Class<D> daoIface;
    private final HandleFactory handleFactory;

    protected BasicDatabaseStoreManager(String databaseType, Class<D> daoIface, HandleFactory handleFactory)
    {
        this.databaseType = databaseType;
        this.daoIface = daoIface;
        this.handleFactory = handleFactory;
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

    public boolean isConflictException(SQLException ex)
    {
        switch (databaseType) {
        case "h2":
            if ("23505".equals(ex.getSQLState())) {
                return true;
            }
            return false;
        default:
            return false;
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

    public <T> T transaction(TransactionAction<T, D> action)
    {
        TransactionState ts = new TransactionState();
        while (true) {
            try (Handle handle = handleFactory.open()) {
                T retval = handle.inTransaction((h, session) -> action.call(h, h.attach(daoIface), ts));
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
            try (Handle handle = handleFactory.open()) {
                T retval = handle.inTransaction((h, session) -> {
                    try {
                        return action.call(h, h.attach(daoIface), ts);
                    }
                    catch (Exception ex) {
                        if (exClass.isAssignableFrom(ex.getClass())) {
                            throw new InnerException(ex);
                        }
                        throw ex;
                    }
                });
                if (ts.retryNext) {
                    ts.retryNext = false;
                    ts.retryCount += 1;
                }
                else {
                    return retval;
                }
            }
            catch (InnerException ex) {
                throw (E) ex.getCause();
            }
        }
    }

    public <T> T autoCommit(AutoCommitAction<T, D> action)
    {
        try (Handle handle = handleFactory.open()) {
            return action.call(handle, handle.attach(daoIface));
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E extends Exception> T autoCommit(AutoCommitActionWithException<T, D, E> action, Class<E> exClass) throws E
    {
        try (Handle handle = handleFactory.open()) {
            return action.call(handle, handle.attach(daoIface));
        }
        catch (InnerException ex) {
            throw (E) ex.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    public <T, E1 extends Exception, E2 extends Exception> T autoCommit(AutoCommitActionWithExceptions<T, D, E1, E2> action, Class<E1> exClass1, Class<E2> exClass2) throws E1, E2
    {
        try (Handle handle = handleFactory.open()) {
            T retval = handle.inTransaction((h, session) -> {
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
