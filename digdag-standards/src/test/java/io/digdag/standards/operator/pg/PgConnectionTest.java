package io.digdag.standards.operator.pg;

import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.ImmutableTableReference;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PgConnectionTest
{
    private static String SQL = "SELECT * FROM users";

    private ResultSetMetaData resultSetMetaData;
    private ResultSet resultSet;
    private Statement statement;
    private Connection connection;
    private PgConnectionConfig pgConnectionConfig;
    private PgConnection pgConnection;

    @Before
    public void setUp()
            throws SQLException
    {
        resultSetMetaData = mock(ResultSetMetaData.class);
        when(resultSetMetaData.getColumnCount()).thenReturn(0);

        resultSet = mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

        statement = mock(Statement.class);
        when(statement.executeQuery(eq(SQL))).thenReturn(resultSet);

        connection = mock(Connection.class);
        when(connection.createStatement()).thenReturn(statement);

        pgConnectionConfig = mock(PgConnectionConfig.class);
        when(pgConnectionConfig.openConnection()).thenReturn(connection);

        pgConnection = spy(PgConnection.open(pgConnectionConfig));
    }

    @Test
    public void executeReadOnlyQuery()
            throws IOException, NotReadOnlyException, SQLException
    {
        AtomicReference<JdbcResultSet> rs = new AtomicReference<>();
        pgConnection.executeReadOnlyQuery(SQL, rs::set);

        verify(pgConnection).execute(eq("SET TRANSACTION READ ONLY"));
        verify(statement).executeQuery(eq(SQL));
        verify(pgConnection).execute(eq("SET TRANSACTION READ WRITE"));
        assertThat(rs.get(), is(notNullValue()));
    }

    @Test
    public void buildInsertStatement()
            throws IOException, NotReadOnlyException, SQLException
    {
        String insertStatement =
                pgConnection.buildInsertStatement(SQL, ImmutableTableReference.builder().schema("myschema").name("desttbl").build());
        assertThat(insertStatement, is("INSERT INTO \"myschema\".\"desttbl\"\n" + SQL));
    }

    @Test
    public void buildCreateTableStatement()
            throws IOException, NotReadOnlyException, SQLException
    {
        String insertStatement =
                pgConnection.buildCreateTableStatement(SQL, ImmutableTableReference.builder().schema("myschema").name("desttbl").build());
        assertThat(insertStatement,
                is("DROP TABLE IF EXISTS \"myschema\".\"desttbl\"; CREATE TABLE \"myschema\".\"desttbl\" AS \n" + SQL));
    }

    @Test
    public void executeScript()
            throws IOException, NotReadOnlyException, SQLException
    {
        pgConnection.executeScript(SQL);
        verify(statement).execute(eq(SQL));
    }

    @Test
    public void executeUpdate()
            throws IOException, NotReadOnlyException, SQLException
    {
        pgConnection.executeUpdate(SQL);
        verify(statement).executeUpdate(eq(SQL));
    }

    @Test
    public void txHelperPrepare()
            throws SQLException, NotReadOnlyException
    {
        TransactionHelper txHelper = pgConnection.getStrictTransactionHelper(null, "__digdag_status", Duration.ofDays(1));
        doThrow(DatabaseException.class).when(pgConnection).executeReadOnlyQuery(eq("SELECT count(*) FROM \"__digdag_status\""), any());
        UUID queryId = UUID.randomUUID();
        txHelper.prepare(queryId);
        verify(pgConnection).execute(eq(
                "CREATE TABLE IF NOT EXISTS \"__digdag_status\"" +
                        " (query_id text NOT NULL UNIQUE, created_at timestamptz NOT NULL, completed_at timestamptz)"));
    }

    private ResultSet setupMockSelectResultSet(UUID queryId)
            throws SQLException
    {
        String selectSQL = "SELECT completed_at FROM \"__digdag_status\" WHERE query_id = '" + queryId + "' FOR UPDATE NOWAIT";
        ResultSet selectRs = mock(ResultSet.class);
        when(statement.executeQuery(eq(selectSQL))).thenReturn(selectRs);
        return selectRs;
    }

    @Test
    public void txHelperLockedTransactionWithNotCompletedStatus()
            throws SQLException, LockConflictException
    {
        UUID queryId = UUID.randomUUID();

        TransactionHelper txHelper = pgConnection.getStrictTransactionHelper(null, "__digdag_status", Duration.ofDays(1));

        // A corresponding status record exists which isn't completed
        ResultSet selectRs = setupMockSelectResultSet(queryId);
        when(selectRs.next()).thenReturn(true);
        when(selectRs.wasNull()).thenReturn(true);

        AtomicBoolean called = new AtomicBoolean(false);
        assertThat(txHelper.lockedTransaction(queryId, () -> called.set(true)), is(true));
        verify(pgConnection).execute(eq("BEGIN"));
        verify(pgConnection).execute(eq("UPDATE \"__digdag_status\" SET completed_at = CURRENT_TIMESTAMP WHERE query_id = '" + queryId + "'"));
        verify(pgConnection).execute(eq("COMMIT"));
    }

    @Test
    public void txHelperLockedTransactionWithCompletedStatus()
            throws SQLException, LockConflictException
    {
        UUID queryId = UUID.randomUUID();

        TransactionHelper txHelper = pgConnection.getStrictTransactionHelper(null, "__digdag_status", Duration.ofDays(1));

        // A corresponding status record exists which is completed
        ResultSet selectRs = setupMockSelectResultSet(queryId);
        when(selectRs.next()).thenReturn(true);
        when(selectRs.wasNull()).thenReturn(false);

        AtomicBoolean called = new AtomicBoolean(false);
        assertThat(txHelper.lockedTransaction(queryId, () -> called.set(true)), is(false));
        verify(pgConnection).execute(eq("BEGIN"));
        verify(pgConnection).execute(eq("ROLLBACK"));
    }
}
