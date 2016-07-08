package io.digdag.standards.operator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Throwables;
import org.postgresql.core.Utils;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import org.slf4j.LoggerFactory;

public abstract class JdbcConnection
        implements AutoCloseable
{
    protected static final Logger logger = LoggerFactory.getLogger(JdbcConnection.class);

    protected final JdbcConnectionConfig config;
    protected final Connection connection;
    protected final DatabaseMetaData databaseMetaData;
    protected String identifierQuoteString;
    protected int fetchSize = 10000; // TODO: Make it configurable

    protected abstract String getClassName();

    protected abstract String getProtocolName();

    public JdbcConnection(JdbcConnectionConfig config)
            throws SQLException
    {
        this.config = config;

        String url = String.format(Locale.ENGLISH, "jdbc:%s://%s:%d/%s", getProtocolName(), config.host(), config.port(), config.database());
        try {
            Class.forName(getClassName());
        }
        catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }

        Properties props = new Properties();
        if (config.schema().isPresent()) {
            props.setProperty("currentSchema", config.schema().get());
        }
        // TODO: Make them configurable
        props.setProperty("loginTimeout", String.valueOf(30));
        props.setProperty("connectTimeout", String.valueOf(30));
        props.setProperty("socketTimeout", String.valueOf(1800));
        props.setProperty("tcpKeepAlive", "true");
        props.setProperty("ssl", String.valueOf(config.ssl()));
        props.setProperty("applicationName", "digdag");

        this.connection = DriverManager.getConnection(url, props);
        this.databaseMetaData = connection.getMetaData();
        this.identifierQuoteString = databaseMetaData.getIdentifierQuoteString();
        connection.setAutoCommit(true);
    }

    public JdbcSchema getSchemaOfQuery(String query) throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            return getSchemaOfResultMetadata(stmt.getMetaData());
        }
    }

    public void executeAndFetchResult(String query, QueryResultHandler resultHandler)
            throws SQLException
    {
        resultHandler.before();
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setFetchSize(fetchSize);

            ResultSet resultSet = statement.executeQuery();
            JdbcSchema schema = getSchemaOfResultMetadata(resultSet.getMetaData());
            resultHandler.schema(schema);

            while (resultSet.next()) {
                ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
                for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                    valuesBuilder.add(resultSet.getObject(i + 1));
                }
                resultHandler.handleRow(valuesBuilder.build());
            }
        }
        finally {
            resultHandler.after();
        }
    }

    protected JdbcSchema getSchemaOfResultMetadata(ResultSetMetaData metadata) throws SQLException
    {
        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for (int i=0; i < metadata.getColumnCount(); i++) {
            int index = i + 1;  // JDBC column index begins from 1
            String name = metadata.getColumnLabel(index);
            String typeName = metadata.getColumnTypeName(index);
            int sqlType = metadata.getColumnType(index);
            int scale = metadata.getScale(index);
            int precision = metadata.getPrecision(index);
            columns.add(new JdbcColumn(name, typeName, sqlType, TypeGroup.getType(sqlType), precision, scale));
        }
        return new JdbcSchema(columns.build());
    }

    @Override
    public void close() throws SQLException
    {
        connection.close();
    }

    public void executeUpdate(String sql) throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    protected String quoteIdentifierString(String str)
    {
        return identifierQuoteString + str + identifierQuoteString;
    }

    public static String escapeIdent(String ident)
    {
        StringBuilder buf = new StringBuilder();
        boolean isFirst = true;
        for (String elm : ident.split("\\.")) {
            if (isFirst) {
                isFirst = false;
            }
            else {
                buf.append(".");
            }

            try {
                Utils.escapeIdentifier(buf, elm);
            }
            catch (SQLException e) {
                logger.warn("Unexpected error occurred: ident=" + ident + ", elm=" + elm, e);
                return ident;
            }
        }
        return buf.toString();
    }

    protected String buildTableName(String tableName)
    {
        return quoteIdentifierString(tableName);
    }

    private boolean tableExists(String tableName) throws SQLException
    {
        try (ResultSet rs = connection.getMetaData().getTables(null, config.schema().orNull(), tableName, null)) {
            return rs.next();
        }
    }

    private Set<String> getColumnNames(String tableName) throws SQLException
    {
        Builder<String> columnNamesBuilder = ImmutableSet.builder();
        try (ResultSet rs = connection.getMetaData().getColumns(null, config.schema().orNull(), tableName, null)) {
            while (rs.next()) {
                columnNamesBuilder.add(rs.getString("COLUMN_NAME"));
            }
            return columnNamesBuilder.build();
        }
    }
}
