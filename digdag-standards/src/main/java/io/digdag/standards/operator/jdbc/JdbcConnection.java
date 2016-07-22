package io.digdag.standards.operator.jdbc;

import java.sql.SQLException;
import java.util.function.Consumer;

public interface JdbcConnection
    extends AutoCloseable
{
    String buildCreateTableStatement(String selectSql, TableReference targetTable);

    String buildInsertStatement(String selectSql, TableReference targetTable);

    void validateStatement(String sql) throws SQLException;

    void executeUpdate(String sql) throws SQLException;

    void executeScript(String sql) throws SQLException;

    void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler) throws SQLException;

    void beginTransaction(String sql) throws SQLException;

    void commitTransaction(String sql) throws SQLException;

    void dropTableIfExists(TableReference ref) throws SQLException;

    default String escapeTableReference(TableReference ref)
    {
        if (ref.getSchema().isPresent()) {
            return escapeIdent(ref.getSchema().get()) + "." + escapeIdent(ref.getName());
        }
        else {
            return escapeIdent(ref.getName());
        }
    }

    String escapeIdent(String ident);

    void close() throws SQLException;
}
