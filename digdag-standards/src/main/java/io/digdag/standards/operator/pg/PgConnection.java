package io.digdag.standards.operator.pg;

import java.util.UUID;
import java.util.function.Consumer;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.util.DurationParam;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnection;
import io.digdag.standards.operator.jdbc.AbstractPersistentTransactionHelper;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import static java.util.Locale.ENGLISH;
import static org.postgresql.core.Utils.escapeIdentifier;

public class PgConnection
    extends AbstractJdbcConnection
{
    protected static PgConnection open(PgConnectionConfig config)
    {
        return new PgConnection(config.openConnection());
    }

    protected PgConnection(Connection connection)
    {
        super(connection);
    }

    @Override
    public void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler)
        throws NotReadOnlyException
    {
        try {
            execute("SET TRANSACTION READ ONLY");
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);  // executeQuery throws exception if given query includes multiple statements
                resultHandler.accept(new PgResultSet(rs));
            }
            execute("SET TRANSACTION READ WRITE");
        }
        catch (SQLException ex) {
            if (ex.getSQLState().equals("25006")) {  // 25006 = read_only_sql_transaction error
                throw new NotReadOnlyException(ex);
            }
            else {
                throw new DatabaseException("Failed to execute given SELECT statement", ex);
            }
        }
    }

    @Override
    public String escapeIdent(String ident)
    {
        try {
            StringBuilder buf = new StringBuilder();
            escapeIdentifier(buf, ident);
            return buf.toString();
        }
        catch (SQLException ex) {
            throw new IllegalArgumentException(
                    String.format(ENGLISH,
                        "Invalid identifier name (%s): %s",
                        ex.getMessage(),
                        ident));
        }
    }

    @Override
    public TransactionHelper getStrictTransactionHelper(String statusTableName, Duration cleanupDuration)
    {
        return new PgPersistentTransactionHelper(statusTableName, cleanupDuration);
    }

    private class PgPersistentTransactionHelper
            extends AbstractPersistentTransactionHelper
    {
        PgPersistentTransactionHelper(String statusTableName, Duration cleanupDuration)
        {
            super(statusTableName, cleanupDuration);
        }

        @Override
        public void prepare()
        {
            executeStatement("create a status table " + escapeIdent(statusTableName),
                    String.format(ENGLISH,
                        "CREATE TABLE IF NOT EXISTS %s" +
                        " (query_id text NOT NULL UNIQUE, created_at timestamptz NOT NULL, completed_at timestamptz)",
                        escapeIdent(statusTableName))
                   );
        }

        @Override
        public void cleanup()
        {
            executeStatement("delete old query status rows from " + escapeIdent(statusTableName) + " table",
                    String.format(ENGLISH,
                        "DELETE FROM %s WHERE query_id = ANY(" +
                        "SELECT query_id FROM %s WHERE completed_at < now() - interval '%d' second" +
                        ")",
                        escapeIdent(statusTableName),
                        escapeIdent(statusTableName),
                        cleanupDuration.getSeconds())
                    );
        }

        @Override
        protected StatusRow lockStatusRow(UUID queryId)
        {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format(ENGLISH,
                            "SELECT completed_at FROM %s WHERE query_id = '%s' FOR UPDATE",
                            escapeIdent(statusTableName),
                            queryId.toString())
                        );
                if (rs.next()) {
                    // status row exists and locked. get status of it.
                    rs.getTimestamp(1);
                    if (rs.wasNull()) {
                        return StatusRow.LOCKED_NOT_COMPLETED;
                    }
                    else {
                        return StatusRow.LOCKED_COMPLETED;
                    }
                }
                else {
                    return StatusRow.NOT_EXISTS;
                }
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to lock a status row", ex);
            }
        }

        @Override
        protected void updateStatusRowAndCommit(UUID queryId)
        {
            executeStatement("update status row",
                    String.format(ENGLISH,
                        "UPDATE %s SET completed_at = now() WHERE query_id = '%s'",
                        escapeIdent(statusTableName),
                        queryId.toString())
                    );
            executeStatement("commit updated status row",
                    "COMMIT");
        }

        @Override
        protected void insertStatusRowAndCommit(UUID queryId)
        {
            try {
                execute(String.format(ENGLISH,
                            "INSERT INTO %s (query_id, created_at) VALUES ('%s', now())",
                            escapeIdent(statusTableName), queryId.toString()));
                // succeeded to insert a status row.
                execute("COMMIT");
            }
            catch (SQLException ex) {
                if (isConflictException(ex)) {
                    // another node inserted a status row after BEGIN call.
                    // skip insert since it already exists.
                    abortTransaction();
                }
                else {
                    throw new DatabaseException("Failed to insert a status row", ex);
                }
            }
        }

        private boolean isConflictException(SQLException ex)
        {
            return "23505".equals(ex.getSQLState());
        }

        @Override
        protected void executeStatement(String desc, String sql)
        {
            try {
                execute(sql);
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to " + desc, ex);
            }
        }
    }
}
