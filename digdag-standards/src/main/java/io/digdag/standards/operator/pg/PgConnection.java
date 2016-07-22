package io.digdag.standards.operator.pg;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Consumer;
import io.digdag.client.config.Config;
import io.digdag.util.DurationParam;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnection;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import static org.postgresql.core.Utils.escapeIdentifier;

public class PgConnection
    extends AbstractJdbcConnection
{
    protected static PgConnection open(PgConnectionConfig config)
        throws SQLException
    {
        return new PgConnection(config.openConnection());
    }

    protected PgConnection(Connection connection)
        throws SQLException
    {
        super(connection);
    }

    @Override
    public void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler)
        throws SQLException
    {
        // TODO call SET TRANSACTION READ ONLY
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);  // executeQuery throws exception if query includes multiple statements
            resultHandler.accept(new PgResultSet(rs));
        }
        // TODO call SET TRANSACTION READ WRITE
    }

    @Override
    public String escapeIdent(String ident)
    {
        try {
            StringBuilder buf = new StringBuilder();
            escapeIdentifier(buf, ident);
            return buf.toString();
        }
        catch (SQLException e) {
            throw new RuntimeException("Invalid identifier name (contains \\0 character): " + ident);
        }
    }
}
