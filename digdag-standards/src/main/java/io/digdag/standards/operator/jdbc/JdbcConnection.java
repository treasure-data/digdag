package io.digdag.standards.operator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import com.google.common.base.Optional;
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

    public void executeQueryAndFetchResult(String query, QueryResultHandler resultHandler)
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

    public void executeQueryWithInsertInto(String query, String destTable)
            throws SQLException
    {
        executeUpdate("INSERT INTO " + escapeIdent(destTable) + "\n" + query);
    }

    public void executeQueryWithCreateTable(String query, String destTable)
            throws SQLException
    {
        String escapedDestName = escapeIdent(destTable);
        executeUpdate("DROP TABLE IF EXISTS " + escapedDestName + "; CREATE TABLE " + escapedDestName + " AS \n" + query);
    }

    public void executeQueryWithUpdateTable(String query, String destTable)
            throws SQLException
    {
        JdbcSchema resultSchema;
        String emptyResultQuery = query + "\n" + "LIMIT 0";
        try (PreparedStatement statement = connection.prepareStatement(emptyResultQuery)) {
            ResultSet resultSet = statement.executeQuery();
            resultSchema = getSchemaOfResultMetadata(resultSet.getMetaData());
        }

        String[] schemaAndTable = destTable.split("\\.");
        String schemaName, tableName;
        if (schemaAndTable.length == 1) {
            schemaName = null;
            tableName = schemaAndTable[0];
        }
        else {
            schemaName = schemaAndTable[0];
            tableName = schemaAndTable[1];
        }
        JdbcSchema destTableSchema = getFromRDB(schemaName, tableName);

        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for(int i = 0; i < resultSchema.getCount(); i++) {
            String columnName = resultSchema.getColumnName(i);
            Optional<JdbcColumn> col = destTableSchema.findByNameIgnoreCase(columnName);
            if (!col.isPresent()) {
                logger.info("Skipping result column {} (the column name does not match any of the columns in the destination table)", columnName);
                continue;
            }
            columns.add(col.get());
        }

        // FIXME: Build a query like this
        //   UPDATE dst_tbl SET name = SRC.name, age = SRC.age, email = SRC.email
        //   FROM (SELECT pid, uid, name, age, email from users where id > 100) SRC
        //   WHERE pid = SRC.pid AND uid = SRC.uid
        //
        //   INSERT INTO dst_tbl (pid, uid, name, age, email)
        //   (SELECT pid, uid, name, age, email from users where id > 100) SRC
        //   WHERE NOT EXISTS (SELECT 1 FROM dst_tbl DST WHERE pid = SRC.pid AND uid = SRC.uid)
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

    public JdbcSchema getFromRDB(String schemaName, String tableName) throws SQLException {
        String escapedSchemaName = escapeIdent(schemaName);
        String escapedTableName = escapeIdent(tableName);
        ResultSet rs = connection.getMetaData().getColumns(null, escapedSchemaName, escapedTableName, null);
        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        try {
            while(rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                int scale = rs.getInt("DECIMAL_DIGITS");
                int precision = rs.getInt("COLUMN_SIZE");
                columns.add(new JdbcColumn(columnName, typeName, sqlType, TypeGroup.fromSqlType(sqlType), precision, scale));
            }
        } finally {
            rs.close();
        }
        return new JdbcSchema(columns.build());
    }

    @Override
    public void close() throws SQLException
    {
        connection.close();
    }

    public String escapeIdent(String ident)
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
