package io.digdag.standards.operator.jdbc;

import java.sql.SQLException;

public class LockConflictException
        extends RuntimeException
{
    public LockConflictException(String message)
    {
        super(message);
    }

    public LockConflictException(SQLException cause)
    {
        super(cause);
    }

    public LockConflictException(String message, SQLException cause)
    {
        super(message, cause);
    }

    @Override
    public SQLException getCause()
    {
        return (SQLException) super.getCause();
    }
}
