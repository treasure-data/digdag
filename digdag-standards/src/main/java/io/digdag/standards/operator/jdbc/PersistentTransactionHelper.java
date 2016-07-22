package io.digdag.standards.operator.jdbc;

import com.google.common.base.Optional;

public class PersistentTransactionHelper
    implements TransactionHelper
{
    private final JdbcConnection connection;
    private final String statusTableName;

    public PersistentTransactionHelper(JdbcConnection connection,
            Optional<String> statusTableName)
    {
        this(connection, statusTableName.or("__digdag_status"));
    }

    public PersistentTransactionHelper(JdbcConnection connection,
            String statusTableName)
    {
        this.connection = connection;
        this.statusTableName = statusTableName;
    }

    @Override
    public void prepare()
    {
        // TODO not implemented yet. create status table
    }

    @Override
    public void cleanup()
    {
        // TODO not implemented yet
    }

    @Override
    public <T> Optional<T> lockedTransaction(TransactionAction<T> action)
    {
        // TODO not implemented yet
        return Optional.of(action.run());
    }
}
