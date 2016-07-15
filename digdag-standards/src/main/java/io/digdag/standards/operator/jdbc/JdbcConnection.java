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

    public void executeQuery(QueryType queryType, String query)
            throws SQLException
    {
        switch (queryType) {
            case WITH_INSERT_INTO:
                executeQueryWithInsertInto(query, config.destTable().get());
                break;
            case WITH_CREATE_TABLE:
                executeQueryWithCreateTable(query, config.destTable().get());
                break;
            case WITH_UPDATE_TABLE:
                executeQueryWithUpdateTable(query, config.destTable().get(), config.uniqKeys().get());
                break;
            default:
                throw new IllegalStateException("Shouldn't reach here: " + queryType);
        }
    }

    public void executeQueryWithInsertInto(String query, String destTable)
            throws SQLException
    {
        executeUpdateWithTransaction(
                "INSERT INTO " + escapeIdent(destTable) + "\n" + query);
    }

    public void executeQueryWithCreateTable(String query, String destTable)
            throws SQLException
    {
        String escapedDestName = escapeIdent(destTable);
        executeUpdateWithTransaction(
                "DROP TABLE IF EXISTS " + escapedDestName + "; CREATE TABLE " + escapedDestName + " AS \n" + query);
    }

    public void executeQueryWithUpdateTable(String query, String destTable, List<String> uniqKeys)
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
        ImmutableList<JdbcColumn> srcColumns = columns.build();

        // In case that the query is "SELECT pid, uid, name, age, email from users where id > 100" and
        // unique keys are [pid, uid]. The query finally constructed is like this:
        //
        //   UPDATE dst_tbl SET name = SRC.name, age = SRC.age, email = SRC.email
        //   FROM (SELECT pid, uid, name, age, email from users where id > 100) SRC
        //   WHERE pid = SRC.pid AND uid = SRC.uid
        //
        //   INSERT INTO dst_tbl (pid, uid, name, age, email)
        //   SELECT * FROM (SELECT pid, uid, name, age, email from users where id > 100) SRC
        //   WHERE NOT EXISTS (SELECT 1 FROM dst_tbl DST WHERE SRC.pid = DST.pid AND SRC.uid = DST.uid)

        String escapedDstTbl = escapeIdent(destTable);
        StringBuilder sql = new StringBuilder();
        // Update statement for existing records
        sql.append("UPDATE ").append(escapedDstTbl).append(" SET ");
        {
            boolean isFirst = true;
            for (JdbcColumn column : srcColumns) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    sql.append(", ");
                }
                String escapedCol = escapeIdent(column.getName());
                sql.append(escapedCol).append(" = SRC.").append(escapedCol);
            }
        }
        sql.append(" FROM (").append(query).append(") SRC ").append("WHERE ");
        {
            boolean isFirst = true;
            for (String uniqKey : uniqKeys) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    sql.append(" AND ");
                }
                String escapedCol = escapeIdent(uniqKey);
                sql.append(escapedDstTbl).append(".").append(escapedCol).append(" = SRC.").append(escapedCol);
            }
        }
        sql.append(";\n");
        // Insert statement for non-existing records
        sql.append("INSERT INTO ").append(escapedDstTbl).append(" (");
        {
            boolean isFirst = true;
            for (JdbcColumn column : srcColumns) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    sql.append(", ");
                }
                String escapedCol = escapeIdent(column.getName());
                sql.append(escapedCol);
            }
        }
        sql.append(")\n");
        sql.append("SELECT * FROM (").append(query).append(") SRC\n").
                append("WHERE NOT EXISTS (SELECT 1 FROM ").append(escapedDstTbl).append(" DST WHERE ");
        {
            boolean isFirst = true;
            for (String uniqKey : uniqKeys) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    sql.append(" AND ");
                }
                String escapedCol = escapeIdent(uniqKey);
                sql.append("SRC.").append(escapedCol).append(" = DST.").append(escapedCol);
            }
        }
        sql.append(");\n");

        // Execute the SQL
        executeUpdateWithTransaction(sql.toString());
    }

    public void executeUpdateWithTransaction(String sql) throws SQLException {
        try {
            executeUpdate("BEGIN");
            if (config.statusTable().isPresent()) {
                updateStatusRecord(Status.RUNNING);
            }
            executeUpdate(sql);
            if (config.statusTable().isPresent()) {
                updateStatusRecord(Status.FINISHED);
            }
            else {
                executeUpdate("SELECT 1");
            }
            executeUpdate("COMMIT");
        }
        catch (Exception e) {
            executeUpdate("ROLLBACK");
        }
    }

    public void executeUpdate(String sql) throws SQLException
    {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
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
        ResultSet rs = connection.getMetaData().getColumns(null, schemaName, tableName, null);
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
            } catch (SQLException e) {
                logger.warn("Unexpected error occurred: ident=" + ident + ", elm=" + elm, e);
                return ident;
            }
        }
        return buf.toString();
    }

    private static class SchemaAndTableName
    {
        String schemaName;
        String tableName;

        public SchemaAndTableName(String schemaName, String tableName)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }
    }

    private SchemaAndTableName parseSchemaAndTableName(String schemaAndTableName)
    {
        String[] schemaAndTableNames = schemaAndTableName.split("\\.");
        SchemaAndTableName result;
        if (schemaAndTableNames.length > 1) {
            result = new SchemaAndTableName(schemaAndTableNames[0], schemaAndTableNames[1]);
        }
        else {
            result = new SchemaAndTableName(null, schemaAndTableNames[0]);
        }
        return result;
    }

    public boolean tableExists(String schemaAndTableName) throws SQLException
    {
        SchemaAndTableName schemaAndTable = parseSchemaAndTableName(schemaAndTableName);
        try (ResultSet rs = connection.getMetaData().getTables(null, schemaAndTable.schemaName,
                                                                schemaAndTable.tableName, null)) {
            return rs.next();
        }
    }

    public void dropTable(String schemaAndTableName) throws SQLException
    {
        executeUpdate("DROP TABLE IF EXISTS" + escapeIdent(schemaAndTableName));
    }

    // Status API
    public enum Status {
        INIT, RUNNING, FINISHED
    }

    public void createStatusTable() throws SQLException
    {
        if (!config.statusTable().isPresent()) {
            return;
        }

        executeUpdate("CREATE TABLE IF NOT EXISTS " + escapeIdent(config.statusTable().get()) +
                "(query_id VARCHAR(64) NOT NULL UNIQUE, time BIGINT NOT NULL, status VARCHAR(16) NOT NULL);\n");
    }

    public Status getStatusRecord() throws SQLException
    {
        if (!config.statusTable().isPresent()) {
            return null;
        }

        String sql = "SELECT status FROM " + escapeIdent(config.statusTable().get()) + " WHERE query_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, config.queryId());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return Status.valueOf(resultSet.getString(1));
            }
            else {
                return null;
            }
        }
        catch (SQLException e) {
            logger.error("SQLException in executeQuery(): " + sql);
            throw e;
        }
    }

    public void addStatusRecord() throws SQLException
    {
        if (!config.statusTable().isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(config.statusTable().get());
        StringBuilder sql = new StringBuilder().append("INSERT INTO ").append(escapedStatusTable).
                                                append(" (query_id, time, status) VALUES (?, ?, ?);\n");
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, config.queryId());
            statement.setLong(i++, System.currentTimeMillis());
            statement.setString(i++, Status.INIT.name());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }

    public void updateStatusRecord(Status status) throws SQLException
    {
        if (!config.statusTable().isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(config.statusTable().get());
        StringBuilder sql = new StringBuilder().append("UPDATE ").append(escapedStatusTable).
                                                append(" SET status = ? WHERE query_id = ?;\n");
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, status.name());
            statement.setString(i++, config.queryId());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }

    public void dropStatusRecord() throws SQLException
    {
        if (!config.statusTable().isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(config.statusTable().get());
        StringBuilder sql = new StringBuilder().append("DELETE FROM ").append(escapedStatusTable).
                                                append(" WHERE query_id = ?");
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, config.queryId());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }

    public static enum QueryType
    {
        SELECT_ONLY(false),
        WITH_INSERT_INTO(true),
        WITH_CREATE_TABLE(true),
        WITH_UPDATE_TABLE(true);

        private boolean isUpdate;

        QueryType(boolean isUpdate)
        {
            this.isUpdate = isUpdate;
        }

        public boolean isUpdate()
        {
            return isUpdate;
        }
    }
}
