package io.digdag.standards.operator.param;

import com.google.common.base.Throwables;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.digdag.client.config.Config;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.database.DatabaseMigrator;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.TransactionFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

public class PostgresqlServerClientConnectionManager
        implements ParamServerClientConnectionManager
{
    private AutoCloseable closer;
    private Config systemConfig;
    private DBI dbi;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public PostgresqlServerClientConnectionManager(Config systemConfig)
    {
        this.systemConfig = systemConfig;

        HikariDataSource ds = createDataSourceWithConnectionPool();
        this.closer = ds;
        this.dbi = new DBI(ds);
        initializeTables();
    }

    @Override
    public ParamServerClientConnection getConnection()
    {
        Handle handle = dbi.open();
        try {
            handle.getConnection().setAutoCommit(false);
        }
        catch (SQLException ex) {
            throw new TransactionFailedException("Failed to set auto commit: " + false, ex);
        }
        handle.begin();

        return new PostgresqlServerClientConnection(handle);
    }

    @Override
    public void shutdown()
    {
        try {
            closer.close();
        }
        catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private HikariDataSource createDataSourceWithConnectionPool()
    {
        DatabaseConfig databaseConfig = DatabaseConfig.convertFrom(systemConfig, "param_server.database");
        String url = DatabaseConfig.buildJdbcUrl(databaseConfig);

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(url);
        hikari.setDriverClassName(DatabaseMigrator.getDriverClassName(databaseConfig.getType()));
        hikari.setDataSourceProperties(DatabaseConfig.buildJdbcProperties(databaseConfig));

        hikari.setConnectionTimeout(databaseConfig.getConnectionTimeout() * 1000);
        hikari.setIdleTimeout(databaseConfig.getIdleTimeout() * 1000);
        hikari.setValidationTimeout(databaseConfig.getValidationTimeout() * 1000);
        hikari.setMaximumPoolSize(databaseConfig.getMaximumPoolSize());
        hikari.setMinimumIdle(databaseConfig.getMinimumPoolSize());

        // Here should not set connectionTestQuery (that overrides isValid) because
        // ThreadLocalTransactionManager.commit assumes that Connection.isValid returns
        // false when an error happened during a transaction.

        logger.debug("Using postgresql URL {}", hikari.getJdbcUrl());

        return new HikariDataSource(hikari);
    }

    private void initializeTables()
    {
        //TODO migrator
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
