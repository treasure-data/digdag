package io.digdag.core;

import javax.sql.DataSource;
import com.google.common.base.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.mchange.v2.c3p0.ComboPooledDataSource;

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

        ComboPooledDataSource ds = new ComboPooledDataSource();

        try {
            ds.setDriverClass(DatabaseMigrator.getDriverClassName(config.getType()));
        } catch (java.beans.PropertyVetoException ex) {
            throw Throwables.propagate(ex);
        }

        ds.setJdbcUrl(config.getUrl());

        //ds.setJdbcUrl(conf.getUrl());
        //ds.setProperties(conf.getProperties());
        //ds.setUser(conf.getUserName());
        //ds.setPassword(conf.getPassword());

        //ds.setAcquireIncrement(conf.getAcquireIncrement());
        //ds.setInitialPoolSize(conf.getInitialPoolSize());
        //ds.setMaxPoolSize(conf.getMaxPoolSize());
        //ds.setMinPoolSize(conf.getMinPoolSize());
        //ds.setMaxConnectionAge(conf.getMaxConnectionAge());
        //ds.setMaxIdleTime(conf.getMaxIdleTime());
        //ds.setMaxIdleTimeExcessConnections(conf.getMaxIdleTimeExcessConnections());
        //ds.setAcquireRetryAttempts(conf.getAcquireRetryAttempts());
        //ds.setAcquireRetryDelay(conf.getAcquireRetryDelay());
        //ds.setCheckoutTimeout(conf.getCheckoutTimeout());
        //ds.setIdleConnectionTestPeriod(conf.getIdleConnectionTestPeriod());
        //ds.setPreferredTestQuery(conf.getPreferredTestQuery());
        //ds.setTestConnectionOnCheckin(conf.getTestConnectionOnCheckin());
        //ds.setTestConnectionOnCheckout(conf.getTestConnectionOnCheckout());
        //ds.setMaxStatements(conf.getMaxStatements());
        //ds.setMaxStatementsPerConnection(conf.getMaxStatementsPerConnection());
        //ds.setDebugUnreturnedConnectionStackTraces(conf.getDebugUnreturnedConnectionStackTraces());
        //ds.setUnreturnedConnectionTimeout(conf.getUnreturnedConnectionTimeout());

        ds.setAutoCommitOnClose(false);

        return ds;
    }
}
