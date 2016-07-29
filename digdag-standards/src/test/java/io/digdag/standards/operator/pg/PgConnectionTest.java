package io.digdag.standards.operator.pg;

import io.digdag.standards.operator.jdbc.ImmutableTableReference;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
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
        PgConnection pgConnection = spy(PgConnection.open(pgConnectionConfig));
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
        PgConnection pgConnection = spy(PgConnection.open(pgConnectionConfig));
        String insertStatement =
                pgConnection.buildInsertStatement(SQL, ImmutableTableReference.builder().schema("myschema").name("desttbl").build());
        assertThat(insertStatement, is("INSERT INTO \"myschema\".\"desttbl\"\n" + SQL));
    }

    @Test
    public void buildCreateTableStatement()
            throws IOException, NotReadOnlyException, SQLException
    {
        PgConnection pgConnection = spy(PgConnection.open(pgConnectionConfig));
        String insertStatement =
                pgConnection.buildCreateTableStatement(SQL, ImmutableTableReference.builder().schema("myschema").name("desttbl").build());
        assertThat(insertStatement,
                is("DROP TABLE IF EXISTS \"myschema\".\"desttbl\"; CREATE TABLE \"myschema\".\"desttbl\" AS \n" + SQL));
    }

    @Test
    public void executeScript()
            throws IOException, NotReadOnlyException, SQLException
    {
        PgConnection pgConnection = spy(PgConnection.open(pgConnectionConfig));
        pgConnection.executeScript(SQL);
        verify(statement).execute(eq(SQL));
    }

    @Test
    public void executeUpdate()
            throws IOException, NotReadOnlyException, SQLException
    {
        PgConnection pgConnection = spy(PgConnection.open(pgConnectionConfig));
        pgConnection.executeUpdate(SQL);
        verify(statement).executeUpdate(eq(SQL));
    }
}