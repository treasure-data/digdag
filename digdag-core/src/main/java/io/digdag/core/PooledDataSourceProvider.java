package io.digdag.core;

import java.util.Properties;
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

    @Inject
    public PooledDataSourceProvider(DatabaseStoreConfig config)
    {
        this.config = config;
    }

    public DataSource get()
    {
        //this.errorRetryLimit = conf.getErrorRetryLimit();
        //this.errorRetryInitialWait = conf.getErrorRetryInitialWait();
        //this.errorRetryWaitLimit = conf.getErrorRetryWaitLimit();
        //this.autoExplainDuration = conf.getAutoExplainDuration();

        Properties props = new Properties();
        props.setProperty("driverClassName", DatabaseMigrator.getDriverClassName(config.getType()));
        props.setProperty("jdbcUrl", config.getUrl());
        HikariConfig hikari = new HikariConfig(props);
        HikariDataSource ds = new HikariDataSource(hikari);

        //ds.setAutoCommitOnClose(false);

        return ds;
    }
}
