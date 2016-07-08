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

import com.google.common.base.Throwables;
import org.postgresql.core.Utils;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
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

    protected abstract String getDriverClassName();

    protected abstract String getProtocolName();

    public JdbcConnection(JdbcConnectionConfig config)
            throws SQLException
    {
        this.config = config;

        String url = String.format(Locale.ENGLISH, "jdbc:%s://%s:%d/%s", getProtocolName(), config.host(), config.port(), config.database());
        try {
            Class.forName(getDriverClassName());
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

    public void executeUpdate(String sql) throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
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
            columns.add(new JdbcColumn(name, typeName, sqlType, TypeGroup.fromSqlType(sqlType), precision, scale));
        }
        return new JdbcSchema(columns.build());
    }

    @Override
    public void close() throws SQLException
    {
        connection.close();
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

    protected boolean tableExists(String tableName) throws SQLException
    {
        try (ResultSet rs = connection.getMetaData().getTables(null, config.schema().orNull(), tableName, null)) {
            return rs.next();
        }
    }
}
