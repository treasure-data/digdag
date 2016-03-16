package io.digdag.core.database;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import com.google.common.base.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariConfig;
import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.database.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledDataSourceProvider
        implements Provider<DataSource>, AutoCloseable
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final DatabaseConfig config;
    private HikariDataSource ds;

    @Inject
    public PooledDataSourceProvider(DatabaseConfig config)
    {
        this.config = config;
    }

    public synchronized DataSource get()
    {
        if (ds == null) {
            //this.errorRetryLimit = conf.getErrorRetryLimit();
            //this.errorRetryInitialWait = conf.getErrorRetryInitialWait();
            //this.errorRetryWaitLimit = conf.getErrorRetryWaitLimit();
            //this.autoExplainDuration = conf.getAutoExplainDuration();

            HikariConfig hikari = new HikariConfig();
            hikari.setJdbcUrl(DatabaseConfig.buildJdbcUrl(config));
            hikari.setDriverClassName(DatabaseMigrator.getDriverClassName(config.getType()));
            hikari.setDataSourceProperties(DatabaseConfig.buildJdbcProperties(config));

            logger.debug("Using database URL {}", hikari.getJdbcUrl());

            ds = new HikariDataSource(hikari);

            //ds.setAutoCommitOnClose(false);
        }

        return ds;
    }

    @PreDestroy
    public synchronized void close()
    {
        if (ds != null) {
            ds.close();
            ds = null;
        }
    }
}
