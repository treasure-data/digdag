package io.digdag.core.database;

import java.util.Properties;
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

            String url = DatabaseConfig.buildJdbcUrl(config);

            logger.debug("Using database URL {}", url);
            Properties props = new Properties();
            props.setProperty("driverClassName", DatabaseMigrator.getDriverClassName(config.getType()));
            props.setProperty("jdbcUrl", url);
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
