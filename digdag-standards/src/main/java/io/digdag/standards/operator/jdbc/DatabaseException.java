package io.digdag.standards.operator.jdbc;

import java.sql.SQLException;

public class DatabaseException
        extends RuntimeException
{
    public DatabaseException(String message)
    {
        super(message);
    }

    public DatabaseException(SQLException cause)
    {
        super(cause);
    }

    public DatabaseException(String message, SQLException cause)
    {
        super(message, cause);
    }

    @Override
    public SQLException getCause()
    {
        return (SQLException) super.getCause();
    }
}
