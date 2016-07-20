package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.postgresql.core.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcQueryHelper
{
    protected static final Logger logger = LoggerFactory.getLogger(JdbcQueryHelper.class);

    protected JdbcConnection jdbcConnection;

    public JdbcQueryHelper(JdbcConnection jdbcConnection)
            throws SQLException
    {
        this.jdbcConnection = jdbcConnection;
    }

    public JdbcSchema getSchemaOfQuery(String query) throws SQLException
    {
        try (PreparedStatement stmt = jdbcConnection.getConnection().prepareStatement(query)) {
            return getSchemaOfResultMetadata(stmt.getMetaData());
        }
    }

    public void executeQueryAndFetchResult(String query, Optional<QueryResultHandler> resultHandler)
            throws SQLException
    {
        if (resultHandler.isPresent()) {
            resultHandler.get().before();
        }
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(query)) {
            statement.setFetchSize(jdbcConnection.config.fetchSize().or(10000));

            try(ResultSet resultSet = statement.executeQuery()) {
                JdbcSchema schema = getSchemaOfResultMetadata(resultSet.getMetaData());
                if (resultHandler.isPresent()) {
                    resultHandler.get().schema(schema);
                }

                while (resultSet.next()) {
                    List<Object> values = new ArrayList<>(resultSet.getMetaData().getColumnCount());
                    for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                        values.add(resultSet.getObject(i + 1));
                    }
                    if (resultHandler.isPresent()) {
                        resultHandler.get().handleRow(values);
                    }
                }
            }
        }
        catch (Exception e) {
            logger.error("SELECT query failed", e);
            throw e;
        }
        finally {
            if (resultHandler.isPresent()) {
                resultHandler.get().after();
            }
        }
    }

    public void executeUpdate(String sql) throws SQLException
    {
        try (Statement stmt = jdbcConnection.getConnection().createStatement()) {
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
        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        try (ResultSet rs = jdbcConnection.getConnection().getMetaData().getColumns(null, schemaName, tableName, null)) {
            while(rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                int scale = rs.getInt("DECIMAL_DIGITS");
                int precision = rs.getInt("COLUMN_SIZE");
                columns.add(new JdbcColumn(columnName, typeName, sqlType, TypeGroup.fromSqlType(sqlType), precision, scale));
            }
        }
        return new JdbcSchema(columns.build());
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
        try (ResultSet rs = jdbcConnection.getConnection().getMetaData().
                                getTables(null, schemaAndTable.schemaName, schemaAndTable.tableName, null)) {
            return rs.next();
        }
    }

    public void dropTable(String schemaAndTableName) throws SQLException
    {
        executeUpdate("DROP TABLE IF EXISTS" + escapeIdent(schemaAndTableName));
    }

    public enum QueryType
    {
        SELECT_ONLY(false),
        UPDATE_QUERY(true),
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
