package io.digdag.standards.operator.jdbc;

import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.ResultSet;

import com.google.common.annotations.VisibleForTesting;
import io.digdag.standards.operator.redshift.RedshiftConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;

public abstract class AbstractJdbcConnection
    implements JdbcConnection
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcConnection.class);

    protected final Connection connection;

    private String quoteString;

    private boolean showQuery;

    protected AbstractJdbcConnection(Connection connection)
    {
        this.connection = connection;
        try {
            connection.setAutoCommit(true);
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to set auto-commit mode to the connection", ex);
        }
        this.showQuery = false;
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
        // Here uses nativeSQL() instead of Connection#prepareStatement because
        // prepareStatement() validates a SQL by creating a server-side prepared statement
        // and RDBMS wrongly decides that the SQL is broken in this case:
        //   * the SQL includes multiple statements
        //   * a statement creates a table and a later statement uses it in the SQL
        try {
            connection.nativeSQL(sql);
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
                loggingExecuteSQL(sql);
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

    private void skipResultSet(ResultSet rs)
        throws SQLException
    {
        while (rs.next())
            ;
    }

    @Override
    public void executeUpdate(String sql)
    {
        try {
            loggingExecuteSQL(sql);
            execute(sql);
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to execute an update statement", ex);
        }
    }

    @VisibleForTesting
    public void execute(String sql)
            throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            loggingExecuteSQL(sql);
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

    @Override
    public void setShowQuery(boolean query){
        showQuery = query;
    }

    @Override
    public boolean getShowQuery(){ return showQuery; }

    protected void loggingExecuteSQL(String sql)
    {
        if( showQuery == false ) {
            return;
        }
        for(String line: sql.split("\r?\n")) {
            logger.info(line);
        }
    }

    protected ResultSet executeQueryWithLogging(String sql) throws SQLException
    {
        Statement stmt = connection.createStatement();
        loggingExecuteSQL(sql);
        return stmt.executeQuery(sql);  // executeQuery throws exception if given query includes multiple statements
    }
}
