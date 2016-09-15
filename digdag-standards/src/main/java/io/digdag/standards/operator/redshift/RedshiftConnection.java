package io.digdag.standards.operator.redshift;

import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.jdbc.TransactionHelper;

import java.util.UUID;
import java.util.function.Consumer;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.util.DurationParam;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnection;
import io.digdag.standards.operator.jdbc.AbstractPersistentTransactionHelper;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import static java.util.Locale.ENGLISH;
import static org.postgresql.core.Utils.escapeIdentifier;

public class RedshiftConnection
    extends PgConnection
{
    static RedshiftConnection open(RedshiftConnectionConfig config)
    {
        return new RedshiftConnection(config.openConnection());
    }

    protected RedshiftConnection(Connection connection)
    {
        super(connection);
    }

    @Override
    public TransactionHelper getStrictTransactionHelper(String statusTableName, Duration cleanupDuration)
    {
        return new RedshiftPersistentTransactionHelper(statusTableName, cleanupDuration);
    }

    public class RedshiftPersistentTransactionHelper
            extends PgPersistentTransactionHelper
    {
        RedshiftPersistentTransactionHelper(String statusTableName, Duration cleanupDuration)
        {
            super(statusTableName, cleanupDuration);
        }

        @Override
        protected String buildCreateTable()
        {
            // Redshift doesn't support timestamptz type. Timestamp type is always UTC.
            return String.format(ENGLISH,
                    "CREATE TABLE IF NOT EXISTS %s" +
                    " (query_id text NOT NULL UNIQUE, created_at timestamp NOT NULL, completed_at timestamp)",
                    escapeIdent(statusTableName));
        }
    }
}
