package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.sql.SQLException;
import java.sql.ResultSet;
import com.google.common.base.*;

public abstract class BasicDatabaseStoreManager
{
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
