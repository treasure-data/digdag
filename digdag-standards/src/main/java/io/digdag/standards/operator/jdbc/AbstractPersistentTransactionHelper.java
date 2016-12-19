package io.digdag.standards.operator.jdbc;

import java.time.Duration;
import java.util.UUID;

public abstract class AbstractPersistentTransactionHelper
    implements TransactionHelper
{
    protected final String statusTableName;
    protected final Duration cleanupDuration;

    protected AbstractPersistentTransactionHelper(String statusTableName, Duration cleanupDuration)
    {
        this.statusTableName = statusTableName;
        this.cleanupDuration = cleanupDuration;
    }

    @Override
    public boolean lockedTransaction(UUID queryId, TransactionAction action)
            throws LockConflictException
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

    private boolean beginTransactionAndLockStatusRow(UUID queryId)
            throws LockConflictException
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

    protected enum StatusRow
    {
        LOCKED_COMPLETED,
        LOCKED_NOT_COMPLETED,
        NOT_EXISTS,
    };

    protected abstract StatusRow lockStatusRow(UUID queryId)
            throws LockConflictException;

    private void beginTransaction()
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
