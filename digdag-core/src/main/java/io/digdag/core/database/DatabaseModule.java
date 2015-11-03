package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Provider;
import javax.sql.DataSource;

import io.digdag.core.*;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.h2.jdbcx.JdbcConnectionPool;

public class DatabaseModule
        implements Module
{
    private final DatabaseStoreConfig config;

    public DatabaseModule(DatabaseStoreConfig config)
    {
        this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DatabaseStoreConfig.class).toInstance(config);
        binder.bind(DataSource.class).toProvider(PooledDataSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(IDBI.class).toProvider(IdbiProvider.class);
        binder.bind(RepositoryStoreManager.class).to(DatabaseRepositoryStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(QueueDescStoreManager.class).to(DatabaseQueueDescStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(SessionStoreManager.class).to(DatabaseSessionStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleStoreManager.class).to(DatabaseScheduleStoreManager.class).in(Scopes.SINGLETON);
    }

    public static class IdbiProvider
            implements Provider<IDBI>
    {
        private final IDBI dbi;

        @Inject
        public IdbiProvider(DataSource ds)
        {
            this.dbi = new DBI(ds);
        }

        public IDBI get()
        {
            return dbi;
        }
    }
}
