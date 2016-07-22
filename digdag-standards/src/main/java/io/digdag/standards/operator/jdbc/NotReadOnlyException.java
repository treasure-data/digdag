package io.digdag.standards.operator.jdbc;

import java.sql.SQLException;

public class NotReadOnlyException
        extends Exception
{
    public NotReadOnlyException(SQLException cause)
    {
        super(cause);
    }

    @Override
    public SQLException getCause()
    {
        return (SQLException) super.getCause();
    }
}
