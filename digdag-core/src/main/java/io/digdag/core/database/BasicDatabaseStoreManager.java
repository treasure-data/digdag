package io.digdag.core.database;

import java.util.List;
import java.util.Date;
import java.sql.SQLException;
import java.sql.ResultSet;
import com.google.common.base.*;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;

public abstract class BasicDatabaseStoreManager
{
    public static <T> T requiredResource(T resource, String messageFormat, Object... messageParameters)
            throws ResourceNotFoundException
    {
        if (resource == null) {
            throw new ResourceNotFoundException("Resource does not exist: " + String.format(messageFormat, messageParameters));
        }
        return resource;
    }

    public interface NewResourceAction <T>
    {
        public T call() throws ResourceConflictException;
    }

    public static <T> T catchConflict(NewResourceAction<T> function,
            String messageFormat, Object... messageParameters)
            throws ResourceConflictException
    {
        try {
            return function.call();
        }
        catch (RuntimeException ex) {
            if (isConflictException(ex)) {
                throw new ResourceConflictException("Resource already exists: " + String.format(messageFormat, messageParameters));
            }
            throw ex;
        }
    }

    public static boolean isConflictException(Exception ex)
    {
        if (ex.toString().contains("Unique index or primary key violation")) {
            // h2 database
            return true;
        }
        return false;
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

    public static Optional<Date> getOptionalDate(ResultSet r, String column)
            throws SQLException
    {
        Date v = r.getDate(column);
        return optional(r.wasNull(), v);
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
