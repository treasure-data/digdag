package io.digdag.core;

import java.util.Properties;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import com.google.common.base.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariConfig;

public class PooledDataSourceProvider
        implements Provider<DataSource>
{
    private final DatabaseStoreConfig config;
    private HikariDataSource ds;

    @Inject
    public PooledDataSourceProvider(DatabaseStoreConfig config)
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

            Properties props = new Properties();
            props.setProperty("driverClassName", DatabaseMigrator.getDriverClassName(config.getType()));
            props.setProperty("jdbcUrl", config.getUrl());
            HikariConfig hikari = new HikariConfig(props);
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
