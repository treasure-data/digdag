package io.digdag.core.database;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.TransactionFailedException;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.util.RetryExecutor;
import io.digdag.util.RetryExecutor.RetryGiveupException;

public abstract class BasicDatabaseStoreManager <D>
{
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final String databaseType;
    private final Class<? extends D> daoIface;
    private final TransactionManager transactionManager;
    protected final ConfigMapper configMapper;

    protected BasicDatabaseStoreManager(
            String databaseType,
            Class<? extends D> daoIface,
            TransactionManager transactionManager,
            ConfigMapper configMapper)
    {
        this.databaseType = databaseType;
        this.daoIface = daoIface;
        this.transactionManager = transactionManager;
        this.configMapper = configMapper;
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

    public <T extends Number> String inLargeIdListExpression(Collection<T> idList)
    {
        if (idList.size() == 1) {
            return "= " + idList.iterator().next().toString();
        }
        else {
            switch (databaseType) {
            case "h2":
                return "in (" +
                    idList.stream()
                    .map(it -> it.toString()).collect(Collectors.joining(", ")) +
                    ")";
            default:
                return "= any('{" +
                    idList.stream()
                    .map(it -> it.toString()).collect(Collectors.joining(",")) +
                    "}')";
            }
        }
    }

    public interface AutoCommitAction <T, D>
    {
        T call(Handle handle, D dao);
    }

    public interface AutoCommitActionWithExceptions <T, D, E1 extends Exception, E2 extends Exception, E3 extends Exception>
    {
        T call(Handle handle, D dao) throws E1, E2, E3;
    }

    public interface TransactionAction <T, D>
    {
        T call(Handle handle, D dao);
    }

    public interface TransactionActionWithExceptions <T, D, E1 extends Exception, E2 extends Exception, E3 extends Exception> {
        T call(Handle handle, D dao) throws E1, E2, E3;
    }

    public <T> T transaction(TransactionAction<T, D> action)
    {
        Handle handle = transactionManager.getHandle(configMapper);
        return action.call(handle, handle.attach(daoIface));
    }

    public <T, E1 extends Exception> T transaction(
            TransactionActionWithExceptions<T, D, E1, RuntimeException, RuntimeException> action,
            Class<E1> exClass1)
        throws E1
    {
        return transaction(action, exClass1, RuntimeException.class, RuntimeException.class);
    }

    public <T, E1 extends Exception, E2 extends Exception> T transaction(
            TransactionActionWithExceptions<T, D, E1, E2, RuntimeException> action,
            Class<E1> exClass1,
            Class<E2> exClass2)
        throws E1, E2
    {
        return transaction(action, exClass1, exClass2, RuntimeException.class);
    }

    public <T, E1 extends Exception, E2 extends Exception, E3 extends Exception> T transaction(
            TransactionActionWithExceptions<T, D, E1, E2, E3> action,
            Class<E1> exClass1,
            Class<E2> exClass2,
            Class<E3> exClass3)
        throws E1, E2, E3
    {
        try {
            Handle handle = transactionManager.getHandle(configMapper);
            return action.call(handle, handle.attach(daoIface));
        }
        catch (Exception ex) {
            Throwables.propagateIfInstanceOf(ex, exClass1);
            Throwables.propagateIfInstanceOf(ex, exClass2);
            Throwables.propagateIfInstanceOf(ex, exClass3);
            Throwables.propagateIfPossible(ex);
            throw new TransactionFailedException(
                    "Transaction failed due to exception being thrown " +
                            "from within the callback. See cause " +
                            "for the original exception.", ex);
        }
    }

    // TODO should be changed naming of this method
    public <T> T autoCommit(AutoCommitAction<T, D> action)
    {
        Handle handle = transactionManager.getHandle(configMapper);
        return action.call(handle, handle.attach(daoIface));
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
