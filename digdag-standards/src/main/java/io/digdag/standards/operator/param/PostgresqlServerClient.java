package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.database.migrate.MigrationContext;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.TransactionFailedException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class PostgresqlServerClient
        implements ParamServerClient
{
    private Config systemConfig;
    private DBI dbi;

    public PostgresqlServerClient(Config sysconfig, DBI dbi)
    {
        this.systemConfig = sysconfig;
        this.dbi = dbi;
        initializeTables();
    }

    @Override
    public Optional<String> get(String key)
    {
        Optional<String> result;
        try (Handle handle = dbi.open()) {
            List<String> record = handle
                    .createQuery("select value from params where key = :key limit 1")
                    .bind("key", key)
                    .mapTo(String.class)
                    .list();
            result = Optional.of(record.get(0));
        }

        return result;
    }

    @Override
    public void set(String key, String value)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.getConnection().setAutoCommit(false);
            }
            catch (SQLException ex) {
                throw new TransactionFailedException("Failed to set auto commit: " + false, ex);
            }

            handle.begin();
            Map<String, Object> firstRecord = handle.createQuery("select key from params where key = :key limit 1 for update ")
                    .bind("key", key).first();
            if (firstRecord == null) {
                handle.insert(
                        "insert into params (key, value, created_at, updated_at) values (?, ?, now(), now())",
                        key, value);
            }
            else {
                handle.update(
                        "update params set value = ?, updated_at = now() where key = ?",
                        value, key);
            }
            handle.commit();
        }
    }

    @Override
    public void finalize()
    {
        // do nothing
    }

    private void initializeTables()
    {
        try (Handle handle = dbi.open()) {
            Map<String, Object> tableExists = handle.createQuery(
                    "select 1 from information_schema.tables where table_name = 'params'"
            ).first();
            if (tableExists != null) {
                return;
            }

            MigrationContext context = new MigrationContext("postgresql");
            handle.update(
                    context.newCreateTableBuilder("params")
                            .addString("key", "not null")
                            .addString("value", "not null")
                            .addTimestamp("updated_at", "not null")
                            .addTimestamp("created_at", "not null")
                            .build()
            );
            handle.update("create unique index params_key_unique_index on params (key)");
        }
    }
}
