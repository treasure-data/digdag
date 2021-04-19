package io.digdag.standards.operator.pg;

import java.util.UUID;
import java.util.function.Consumer;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import com.google.common.annotations.VisibleForTesting;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnection;
import io.digdag.standards.operator.jdbc.AbstractPersistentTransactionHelper;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.TableReference;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import static java.util.Locale.ENGLISH;
import static org.postgresql.core.Utils.escapeIdentifier;

public class PgConnection
    extends AbstractJdbcConnection
{
    @VisibleForTesting
    public static PgConnection open(PgConnectionConfig config)
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
    public TransactionHelper getStrictTransactionHelper(String statusTableSchema, String statusTableName, Duration cleanupDuration)
    {
        return new PgPersistentTransactionHelper(statusTableSchema, statusTableName, cleanupDuration);
    }

    private class PgPersistentTransactionHelper
            extends AbstractPersistentTransactionHelper
    {
        private final TableReference statusTableReference;

        PgPersistentTransactionHelper(String statusTableSchema, String statusTableName, Duration cleanupDuration)
        {
            super(cleanupDuration);
            if (statusTableSchema != null) {
                statusTableReference = TableReference.of(statusTableSchema, statusTableName);
            }
            else {
                statusTableReference = TableReference.of(statusTableName);
            }
        }

        TableReference statusTableReference()
        {
            return statusTableReference;
        }

        String buildCreateTable()
        {
            return String.format(ENGLISH,
                    "CREATE TABLE IF NOT EXISTS %s" +
                    " (query_id text NOT NULL UNIQUE, created_at timestamptz NOT NULL, completed_at timestamptz)",
                    escapeTableReference(statusTableReference()));
        }

        @Override
        public void prepare(UUID queryId)
        {
            String sql = buildCreateTable();
            executeStatement("create a status table " + escapeTableReference(statusTableReference()) + ".\n"
                            + "hint: if you don't have permission to create tables, "
                            + "please try one of these options:\n"
                            + "1. add 'strict_transaction: false' option to disable "
                            + "exactly-once transaction control that depends on this table.\n"
                            + "2. ask system administrator to create this table using the following command "
                            + "and grant SELECT/INSERT/UPDATE/DELETE or ALL privileges to this user: " + sql + ";\n"
                            + "3. ask system administrator to create a schema that this user can create a table "
                            + "and set 'status_table_schema' option to it\n"
                            , sql);
        }

        @Override
        public void cleanup()
        {
            executeStatement("delete old query status rows from " + escapeTableReference(statusTableReference()) + " table",
                    String.format(ENGLISH,
                        "DELETE FROM %s WHERE query_id = ANY(" +
                        "SELECT query_id FROM %s WHERE completed_at < now() - interval '%d' second" +
                        ")",
                        escapeTableReference(statusTableReference()),
                        escapeTableReference(statusTableReference()),
                        cleanupDuration.getSeconds())
                    );
        }

        @Override
        protected StatusRow lockStatusRow(UUID queryId)
                throws LockConflictException
        {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format(ENGLISH,
                            "SELECT completed_at FROM %s WHERE query_id = '%s' FOR UPDATE NOWAIT",
                            escapeTableReference(statusTableReference()),
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
                if (ex.getSQLState().equals("55P03")) {
                    throw new LockConflictException("Failed to acquire a status row lock", ex);
                }
                else {
                    throw new DatabaseException("Failed to lock a status row", ex);
                }
            }
        }

        @Override
        protected void updateStatusRowAndCommit(UUID queryId)
        {
            executeStatement("update status row",
                    String.format(ENGLISH,
                        "UPDATE %s SET completed_at = CURRENT_TIMESTAMP WHERE query_id = '%s'",
                        escapeTableReference(statusTableReference()),
                        queryId.toString())
                    );
            executeStatement("commit updated status row", "COMMIT");
        }

        @Override
        protected void insertStatusRowAndCommit(UUID queryId)
        {
            try {
                execute(String.format(ENGLISH,
                            "INSERT INTO %s (query_id, created_at) VALUES ('%s', CURRENT_TIMESTAMP)",
                            escapeTableReference(statusTableReference()), queryId.toString()));
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

        boolean isConflictException(SQLException ex)
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
