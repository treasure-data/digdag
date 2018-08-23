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
    public Optional<String> get(String key, int siteId)
    {
        Optional<String> result;
        try (Handle handle = dbi.open()) {
            List<String> record = handle
                    .createQuery("select value from params where key = :key and site_id = :site_id limit 1")
                    .bind("key", key)
                    .bind("site_id", siteId)
                    .mapTo(String.class)
                    .list();

            if (record.size() > 0) {
                result = Optional.of(record.get(0));
            }
            else {
                result = Optional.absent();
            }
        }

        return result;
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.getConnection().setAutoCommit(false);
            }
            catch (SQLException ex) {
                throw new TransactionFailedException("Failed to set auto commit: " + false, ex);
            }

            handle.begin();

            handle.insert(
               "insert into params (key, value, site_id, created_at, updated_at) values (?, ?, ?, now(), now()) " +
               "on conflict on constraint params_site_id_key_uniq do update set value = ?, set updated_at = now()",
                key, value, siteId, siteId);
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

            handle.execute(
                "CREATE TABLE params (" +
                        "key text NOT NULL," +
                        "value text NOT NULL," +
                        "site_id integer," +
                        "updated_at timestamp with time zone NOT NULL," +
                        "created_at timestamp with time zone NOT NULL," +
                        "CONSTRAINT params_site_id_key_uniq UNIQUE(site_id, key)" +
                    ")"
            );
        }
    }
}
