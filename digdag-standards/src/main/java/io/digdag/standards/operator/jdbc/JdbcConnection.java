package io.digdag.standards.operator.jdbc;

import java.time.Duration;
import java.util.function.Consumer;

interface JdbcConnection
    extends AutoCloseable
{
    String buildCreateTableStatement(String selectSql, TableReference targetTable);

    String buildInsertStatement(String selectSql, TableReference targetTable);

    Exception validateStatement(String sql);

    void executeScript(String sql);

    void executeUpdate(String sql);

    void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler)
        throws NotReadOnlyException;

    TransactionHelper getStrictTransactionHelper(String statusTableSchema, String statusTableName, Duration cleanupDuration);

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

    void setDebug(Boolean debug);
    Boolean getDebug();

    void close();
}
