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

    //
    // Digdag <= v0.9.34
    // Previous versions doesn't have those methods for backward compatibility.
    //
    default void setShowQuery(boolean query) { new RuntimeException("Subclass need to implement this method."); }
    default boolean getShowQuery() { return false; }

    void close();
}
