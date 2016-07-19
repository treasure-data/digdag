package io.digdag.standards.operator.postgresql;

import io.digdag.standards.operator.jdbc.JdbcConnection;
import io.digdag.standards.operator.jdbc.JdbcConnectionConfig;

import java.sql.SQLException;

class PostgreSQLConnection
        extends JdbcConnection
{
    public PostgreSQLConnection(JdbcConnectionConfig config)
            throws SQLException
    {
        super(config);
    }

    @Override
    protected String getDriverClassName()
    {
        return "org.postgresql.Driver";
    }

    @Override
    protected String getProtocolName()
    {
        return "postgresql";
    }
}
