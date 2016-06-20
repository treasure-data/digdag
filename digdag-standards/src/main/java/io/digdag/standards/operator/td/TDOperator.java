package io.digdag.standards.operator.td;

import java.util.Date;
import java.io.Closeable;

import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.util.RetryExecutor;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.TDClientHttpUnauthorizedException;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDExportJobRequest;
import static io.digdag.util.RetryExecutor.retryExecutor;

public class TDOperator
        implements Closeable
{
    public static TDOperator fromConfig(Config config)
    {
        String database = config.get("database", String.class).trim();
        if (database.isEmpty()) {
            throw new ConfigException("Parameter 'database' is empty");
        }

        TDClient client = TDClientFactory.clientFromConfig(config);

        return new TDOperator(client, database);
    }

    static final RetryExecutor defaultRetryExecutor = retryExecutor()
        .retryIf((exception) -> !isDeterministicClientException(exception));

    public static String escapeHiveIdent(String ident)
    {
        // TODO escape symbols in ident
        return "`" + ident + "`";
    }

    public static String escapePrestoIdent(String ident)
    {
        // TODO escape symbols in ident
        return "\"" + ident + "\"";
    }

    public static String escapeHiveTableName(TableParam table)
    {
        if (table.getDatabase().isPresent()) {
            return escapeHiveIdent(table.getDatabase().get()) + '.' + escapeHiveIdent(table.getTable());
        }
        else {
            return escapeHiveIdent(table.getTable());
        }
    }

    public static String escapePrestoTableName(TableParam table)
    {
        if (table.getDatabase().isPresent()) {
            return escapePrestoIdent(table.getDatabase().get()) + '.' + escapePrestoIdent(table.getTable());
        }
        else {
            return escapePrestoIdent(table.getTable());
        }
    }

    private final TDClient client;
    private final String database;

    protected TDOperator(TDClient client, String database)
    {
        this.client = client;
        this.database = database;
    }

    public TDOperator withDatabase(String anotherDatabase)
    {
        return new TDOperator(client, anotherDatabase);
    }

    public String getDatabase()
    {
        return database;
    }

    public void ensureDatabaseCreated(String name)
        throws TDClientException
    {
        try {
            defaultRetryExecutor.run(() -> client.createDatabase(name));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpConflictException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public void ensureDatabaseDeleted(String name)
        throws TDClientException
    {
        try {
            defaultRetryExecutor.run(() -> client.deleteDatabase(name));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpNotFoundException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public void ensureTableCreated(String tableName)
        throws TDClientException
    {
        try {
            // TODO set include_v=false option
            defaultRetryExecutor.run(() -> client.createTable(database, tableName));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpConflictException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public void ensureTableDeleted(String tableName)
        throws TDClientException
    {
        try {
            // TODO set include_v=false option
            defaultRetryExecutor.run(() -> client.deleteTable(database, tableName));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpNotFoundException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public boolean tableExists(String table)
    {
        return client.existsTable(database, table);
    }

    public TDJobOperator submitNewJob(TDJobRequest request)
    {
        // TODO retry with an unique id and ignore conflict
        return newJobOperator(client.submit(request));
    }

    public TDJobOperator submitExportJob(TDExportJobRequest request)
    {
        // TODO retry with an unique id and ignore conflict
        return newJobOperator(client.submitExportJob(request));
    }

    public TDJobOperator startSavedQuery(String name, Date scheduledTime)
    {
        // TODO retry with an unique id and ignore conflict
        return newJobOperator(client.startSavedQuery(name, scheduledTime));
    }

    public TDJobOperator newJobOperator(String jobId)
    {
        return new TDJobOperator(client, jobId);
    }

    static boolean isDeterministicClientException(Exception ex)
    {
        return ex instanceof TDClientHttpNotFoundException ||
            ex instanceof TDClientHttpConflictException ||
            ex instanceof TDClientHttpUnauthorizedException;
    }

    @Override
    public void close()
    {
        client.close();
    }
}
