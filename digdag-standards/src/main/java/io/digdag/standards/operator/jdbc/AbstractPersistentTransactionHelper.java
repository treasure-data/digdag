package io.digdag.standards.operator.jdbc;

import java.util.UUID;

public abstract class AbstractPersistentTransactionHelper
    implements TransactionHelper
{
    protected final String statusTableName;

    public AbstractPersistentTransactionHelper(String statusTableName)
    {
        this.statusTableName = statusTableName;
    }

    @Override
    public boolean lockedTransaction(UUID queryId, TransactionAction action)
    {
        boolean completed = beginTransactionAndLockStatusRow(queryId);

        // status row is locked here until this transaction is committed or aborted.

        if (completed) {
            // query was completed successfully before. skip the action.
            abortTransaction();
            return false;
        }
        else {
            // query is not completed. run the action.
            action.run();
            updateStatusRowAndCommit(queryId);
            return true;
        }
    }

    protected boolean beginTransactionAndLockStatusRow(UUID queryId)
    {
        do {
            beginTransaction();

            StatusRow status = lockStatusRow(queryId);
            switch (status) {
            case LOCKED_COMPLETED:
                return true;
            case LOCKED_NOT_COMPLETED:
                return false;
            case NOT_EXISTS:
                // status row doesn't exist. insert one.
                insertStatusRowAndCommit(queryId);
            }
        } while (true);
    }

    protected static enum StatusRow
    {
        LOCKED_COMPLETED,
        LOCKED_NOT_COMPLETED,
        NOT_EXISTS,
    };

    protected abstract StatusRow lockStatusRow(UUID queryId);

    protected void beginTransaction()
    {
        executeStatement("begin a transaction", "BEGIN");
    }

    protected void abortTransaction()
    {
        executeStatement("rollback a transaction", "ROLLBACK");
    }

    protected abstract void updateStatusRowAndCommit(UUID queryId);

    protected abstract void insertStatusRowAndCommit(UUID queryId);

    protected abstract void executeStatement(String desc, String sql);
}
