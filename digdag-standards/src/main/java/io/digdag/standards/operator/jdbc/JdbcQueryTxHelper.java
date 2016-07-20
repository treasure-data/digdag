package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcQueryTxHelper
    extends JdbcQueryHelper
{
    protected static final Logger logger = LoggerFactory.getLogger(JdbcQueryTxHelper.class);

    protected String queryId;
    protected Optional<String> statusTable;

    public JdbcQueryTxHelper(JdbcConnection jdbcConnection, String queryId, Optional<String> statusTable)
            throws SQLException
    {
        super(jdbcConnection);
        this.queryId = queryId;
        this.statusTable = statusTable;
    }

    public void executeQuery(
            QueryType queryType,
            String query,
            Optional<String> destTable,
            Optional<List<String>> uniqKeys,
            Optional<QueryResultHandler> queryResultHandler)
            throws Exception
    {
        switch (queryType) {
            case SELECT_ONLY:
                executeQueryWithTransaction(() -> executeQueryAndFetchResult(query, queryResultHandler.get()));
                break;
            case UPDATE_QUERY:
                executeQueryWithTransaction(() -> executeUpdate(query));
                break;
            case WITH_INSERT_INTO:
                executeQueryWithInsertInto(query, destTable.get());
                break;
            case WITH_CREATE_TABLE:
                executeQueryWithCreateTable(query, destTable.get());
                break;
            case WITH_UPDATE_TABLE:
                executeQueryWithUpdateTable(query, destTable.get(), uniqKeys.get());
                break;
            default:
                throw new IllegalStateException("Shouldn't reach here: " + queryType);
        }
    }

    public void executeQueryWithInsertInto(String query, String destTable)
            throws Exception
    {
        String sql = "INSERT INTO " + escapeIdent(destTable) + "\n" + query;
        executeQueryWithTransaction(() -> executeUpdate(sql));
    }

    public void executeQueryWithCreateTable(String query, String destTable)
            throws Exception
    {
        String escapedDestName = escapeIdent(destTable);
        String sql = "DROP TABLE IF EXISTS " + escapedDestName + "; CREATE TABLE " + escapedDestName + " AS \n" + query;
        executeQueryWithTransaction(() -> executeUpdate(sql));
    }

    public void executeQueryWithUpdateTable(String query, String destTable, List<String> uniqKeys)
            throws Exception
    {
        JdbcSchema resultSchema;
        String emptyResultQuery = query + "\n" + "LIMIT 0";
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(emptyResultQuery)) {
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
        executeQueryWithTransaction(() -> executeUpdate(sql.toString()));
    }

    interface QueryExecutor
    {
        void execute() throws SQLException;
    }

    public void executeQueryWithTransaction(QueryExecutor queryExecutor)
            throws Exception
    {
        try {
            executeUpdate("BEGIN");

            // If Digdag doesn't guarantee to prepend multiple workers from running the same workflow simultaneously, we need to use this lock
            // tryLockStatusRecord();

            logger.debug("Starting query");
            queryExecutor.execute();
            logger.debug("Query finished");

            if (statusTable.isPresent()) {
                updateStatusRecord(Status.FINISHED);
            }
            else {
                executeUpdate("SELECT 1");
            }
            executeUpdate("COMMIT");
        }
        catch (Exception e) {
            logger.error("Query failed. Rolling back...", e);
            executeUpdate("ROLLBACK");
            throw e;
        }
    }

    // Status API
    public enum Status
    {
        READY, FINISHED
    }

    public void createStatusTable() throws SQLException
    {
        if (!statusTable.isPresent()) {
            return;
        }

        executeUpdate("CREATE TABLE IF NOT EXISTS " + escapeIdent(statusTable.get()) +
                "(query_id VARCHAR(64) NOT NULL UNIQUE, time BIGINT NOT NULL, status VARCHAR(16) NOT NULL);\n");
    }

    public Status getStatusRecord() throws SQLException
    {
        if (!statusTable.isPresent()) {
            return null;
        }

        String sql = "SELECT status FROM " + escapeIdent(statusTable.get()) + " WHERE query_id = ?";
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(sql)) {
            statement.setString(1, queryId);
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
        if (!statusTable.isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(statusTable.get());
        StringBuilder sql = new StringBuilder().append("INSERT INTO ").append(escapedStatusTable).
                                                append(" (query_id, time, status) VALUES (?, ?, ?);\n");
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, queryId);
            statement.setLong(i++, System.currentTimeMillis());
            statement.setString(i++, Status.READY.name());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }

    public void updateStatusRecord(Status status) throws SQLException
    {
        if (!statusTable.isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(statusTable.get());
        StringBuilder sql = new StringBuilder().append("UPDATE ").append(escapedStatusTable).
                                                append(" SET status = ? WHERE query_id = ?;\n");
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, status.name());
            statement.setString(i++, queryId);
            statement.executeUpdate();
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }

    public void tryLockStatusRecord() throws SQLException
    {
        if (!statusTable.isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(statusTable.get());
        StringBuilder sql = new StringBuilder().append("SELECT * FROM ").append(escapedStatusTable).
                                                append(" WHERE query_id = ? FOR UPDATE NOWAIT");
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, queryId);
            ResultSet resultSet = statement.executeQuery();
            if (!resultSet.next()) {
                throw new IllegalStateException("The status row doesn't exist unexpectedly");
            }
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }

    public void dropStatusRecord() throws SQLException
    {
        if (!statusTable.isPresent()) {
            return;
        }

        String escapedStatusTable = escapeIdent(statusTable.get());
        StringBuilder sql = new StringBuilder().append("DELETE FROM ").append(escapedStatusTable).
                                                append(" WHERE query_id = ?");
        try (PreparedStatement statement = jdbcConnection.getConnection().prepareStatement(sql.toString())) {
            int i = 1;
            statement.setString(i++, queryId);
            statement.executeUpdate();
        }
        catch (SQLException e) {
            logger.error("SQLException in executeUpdate(): " + sql);
            throw e;
        }
    }
}
