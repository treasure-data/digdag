package io.digdag.standards.task.td;

import java.io.Closeable;
import io.digdag.client.config.Config;
import io.digdag.standards.task.BaseTaskRunner;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientBuilder;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpNotFoundException;

public class TDOperation
        implements Closeable
{
    public static TDOperation fromConfig(Config config)
    {
        Config apiConfig = config.getNestedOrGetEmpty("td")
            .deepCopy()
            .setAll(config);

        String database = apiConfig.get("database", String.class);

        TDClientBuilder builder = new TDClientBuilder(false);
        builder.setEndpoint(apiConfig.get("endpoint", String.class, "api.treasuredata.com"));
        builder.setUseSSL(apiConfig.get("use_ssl", boolean.class, true));
        builder.setApiKey(apiConfig.get("apikey", String.class));

        return new TDOperation(builder.build(), database);
    }

    private final TDClient client;
    private final String database;

    public TDOperation(TDClient client, String database)
    {
        this.client = client;
        this.database = database;
    }

    public TDClient getClient()
    {
        return client;
    }

    public String getDatabase()
    {
        return database;
    }

    public void ensureTableCreated(String tableName)
        throws TDClientException
    {
        try {
            // TODO set include_v=false option
            client.createTable(database, tableName);
        }
        catch (TDClientHttpConflictException ex) {
            // ignore
        }
    }

    public static String escapeIdentHive(String ident)
    {
        // TODO escape symbols in ident
        return "`" + ident + "`";
    }

    public static String escapeIdentPresto(String ident)
    {
        // TODO escape symbols in ident
        return "\"" + ident + "\"";
    }

    public void ensureTableDeleted(String tableName)
        throws TDClientException
    {
        try {
            client.deleteTable(database, tableName);
        }
        catch (TDClientHttpNotFoundException ex) {
            // ignore
        }
    }

    @Override
    public void close()
    {
        client.close();
    }
}
