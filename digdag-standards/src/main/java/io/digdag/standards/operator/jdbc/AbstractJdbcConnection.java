package io.digdag.standards.operator.jdbc;

import java.util.Properties;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;

public abstract class AbstractJdbcConnection
    implements JdbcConnection
{
    protected final Connection connection;

    private String quoteString;

    public AbstractJdbcConnection(Connection connection)
        throws SQLException
    {
        this.connection = connection;
        connection.setAutoCommit(true);
    }

    @Override
    public String buildCreateTableStatement(String selectSql, TableReference targetTable)
    {
        String escapedRef = escapeTableReference(targetTable);
        return String.format(ENGLISH,
                "DROP TABLE IF EXISTS %s; CREATE TABLE %s AS \n%s",
                escapedRef, escapedRef, selectSql);
    }

    @Override
    public String buildInsertStatement(String selectSql, TableReference targetTable)
    {
        String escapedRef = escapeTableReference(targetTable);
        return String.format(ENGLISH,
                "INSERT INTO %s\n%s",
                escapedRef, selectSql);
    }

    @Override
    public void validateStatement(String sql)
        throws SQLException
    {
        // if JDBC or DBMS does not use server-side prepared statements by default,
        // subclass needs to override this method.
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
        }
    }

    @Override
    public void executeScript(String sql)
        throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            boolean hasResults = stmt.execute(sql);
            while (hasResults) {
                ResultSet rs = stmt.getResultSet();
                skipResultSet(rs);
                rs.close();
                hasResults = stmt.getMoreResults();
            }
        }
    }

    protected void skipResultSet(ResultSet rs)
        throws SQLException
    {
        while (rs.next())
            ;
    }

    @Override
    public void executeUpdate(String sql)
        throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    @Override
    public void beginTransaction(String sql)
        throws SQLException
    {
        executeUpdate("begin");
    }

    @Override
    public void commitTransaction(String sql)
        throws SQLException
    {
        executeUpdate("commit");
    }

    @Override
    public void dropTableIfExists(TableReference ref)
        throws SQLException
    {
        executeUpdate("DROP TABLE IF EXISTS " + escapeTableReference(ref));
    }

    @Override
    public String escapeIdent(String ident)
    {
        if (quoteString == null) {
            try {
                DatabaseMetaData meta = connection.getMetaData();
                quoteString = meta.getIdentifierQuoteString();
            }
            catch (SQLException ex) {
                throw new RuntimeException("Failed to retrieve database metadata to quote an identifier name", ex);
            }
        }
        return quoteString + ident.replaceAll(Pattern.quote(quoteString), quoteString + quoteString) + quoteString;
    }

    @Override
    public void close()
        throws SQLException
    {
        connection.close();
    }
}
