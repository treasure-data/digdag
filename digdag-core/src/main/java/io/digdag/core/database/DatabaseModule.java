package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Provider;
import javax.sql.DataSource;

import io.digdag.core.queue.QueueSettingStoreManager;
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
        binder.bind(IDBI.class).toProvider(IdbiProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConfigMapper.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseMigrator.class).in(Scopes.SINGLETON);
        binder.bind(RepositoryStoreManager.class).to(DatabaseRepositoryStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(QueueSettingStoreManager.class).to(DatabaseQueueSettingStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(SessionStoreManager.class).to(DatabaseSessionStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleStoreManager.class).to(DatabaseScheduleStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseTaskQueueStore.class).in(Scopes.SINGLETON);
    }

    public static class IdbiProvider
            implements Provider<IDBI>
    {
        private final IDBI dbi;

        @Inject
        public IdbiProvider(DataSource ds, DatabaseConfig config)
        {
            this.dbi = new DBI(ds);

            if (config.getAutoMigrate()) {
                new DatabaseMigrator(dbi, config).migrate();
            }
        }

        public IDBI get()
        {
            return dbi;
        }
    }
}
