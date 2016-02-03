package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Provider;
import javax.sql.DataSource;

import io.digdag.core.queue.QueueDescStoreManager;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.session.SessionStoreManager;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.h2.jdbcx.JdbcConnectionPool;

public class DatabaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DatabaseConfig.class).toProvider(DatabaseConfigProvider.class).in(Scopes.SINGLETON);
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
