package io.digdag.core.database;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.SQLException;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.h2.jdbcx.JdbcDataSource;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariConfig;
import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.database.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceProvider
        implements Provider<DataSource>, AutoCloseable
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final DatabaseConfig config;
    private DataSource ds;
    private AutoCloseable closer;

    @Inject
    public DataSourceProvider(DatabaseConfig config)
    {
        this.config = config;
    }

    public synchronized DataSource get()
    {
        if (ds == null) {
            switch (config.getType()) {
            case "h2":
                // h2 database doesn't need connection pool
                createSimpleDataSource();
                break;
            default:
                createPooledDataSource();
                break;
            }
        }
        return ds;
    }

    private void createSimpleDataSource()
    {
        String url = DatabaseConfig.buildJdbcUrl(config);

        // H2 closes the database when all of the connections are closed by default. When database
        // is closed, data is gone with in-memory mode. It's unexpected. However, if here disables
        // that behavior using DB_CLOSE_DELAY=-1 option, there're no methods to close the database
        // explicitly. Only method to close is to not disable shutdown hook of H2 database
        // (DB_CLOSE_ON_EXIT=TRUE). But this is also causes unexpected behavior if PreDestroy is
        // triggered at shutdown hook. Therefore, here needs to depend on injector to resolve
        // dependencies so that database is closed after calling all other PreDestroy methods that
        // depend on DataSourceProvider.
        // To solve this issue, here holds one Connection without using DB_CLOSE_DELAY=-1 option.
        JdbcDataSource ds = new JdbcDataSource();
        ds.setUrl(url + ";DB_CLOSE_ON_EXIT=FALSE");

        logger.debug("Using database URL {}", url);

        try {
            this.closer = ds.getConnection();
        }
        catch (SQLException ex) {
            throw Throwables.propagate(ex);
        }
        this.ds = ds;
    }

    private void createPooledDataSource()
    {
        String url = DatabaseConfig.buildJdbcUrl(config);

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(url);
        hikari.setDriverClassName(DatabaseMigrator.getDriverClassName(config.getType()));
        hikari.setDataSourceProperties(DatabaseConfig.buildJdbcProperties(config));

        hikari.setConnectionTimeout(config.getConnectionTimeout() * 1000);
        hikari.setIdleTimeout(config.getIdleTimeout() * 1000);
        hikari.setValidationTimeout(config.getValidationTimeout() * 1000);
        hikari.setMaximumPoolSize(config.getMaximumPoolSize());

        // Here should not set connectionTestQuery (that overwrites isValid) because
        // BasicDatabaseStoreManager.validateTransactionAndCommit assumes that
        // Connection.isValid returns false when an error happened during a transaction.

        logger.debug("Using database URL {}", hikari.getJdbcUrl());

        HikariDataSource ds = new HikariDataSource(hikari);
        this.ds = ds;
        this.closer = ds;
    }

    @PreDestroy
    public synchronized void close()
    {
        if (ds != null) {
            try {
                closer.close();
            }
            catch (Exception ex) {
                throw Throwables.propagate(ex);
            }
            ds = null;
        }
    }
}
