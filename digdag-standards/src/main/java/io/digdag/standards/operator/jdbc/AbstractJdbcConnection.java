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
    private static final Logger logger = LoggerFactory.getLogger(JdbcConnection.class);

    protected final Connection connection;

    private String quoteString;

    public AbstractJdbcConnection(Connection connection)
    {
        this.connection = connection;
        try {
            connection.setAutoCommit(true);
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to set auto-commit mode to the connection", ex);
        }
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
    public SQLException validateStatement(String sql)
    {
        // if JDBC or DBMS does not use server-side prepared statements by default,
        // subclass needs to override this method.
        try {
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.getMetaData();
            }
            return null;
        }
        catch (SQLException ex) {
            if (ex.getSQLState().startsWith("42")) {
                // SQL error class 42
                return ex;
            }
            throw new DatabaseException("Failed to validate statement", ex);
        }
    }

    @Override
    public void executeScript(String sql)
    {
        try {
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
        catch (SQLException ex) {
            throw new DatabaseException("Failed to execute given SQL script", ex);
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
    {
        try {
            execute(sql);
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to execute an update statement", ex);
        }
    }

    protected void execute(String sql)
        throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
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
                throw new DatabaseException("Failed to retrieve database metadata to quote an identifier name", ex);
            }
        }
        return quoteString + ident.replaceAll(Pattern.quote(quoteString), quoteString + quoteString) + quoteString;
    }

    @Override
    public void close()
    {
        try {
            connection.close();
        }
        catch (SQLException ex) {
            logger.warn("Failed to close a database connection. Ignoring.", ex);
        }
    }
}
