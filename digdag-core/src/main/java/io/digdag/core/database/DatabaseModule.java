package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.Provider;
import javax.sql.DataSource;
import javax.annotation.PostConstruct;
import io.digdag.core.queue.QueueSettingStoreManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.session.SessionStoreManager;
import org.skife.jdbi.v2.DBI;

public class DatabaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DatabaseConfig.class).toProvider(DatabaseConfigProvider.class).in(Scopes.SINGLETON);
        binder.bind(DataSource.class).toProvider(DataSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(AutoMigrator.class);

        // Here makes DBI singletone because all BasicDatabaseStoreManager classes must share a DBI instance
        // so that all transactions can use all registered mappers and argument factories (dbi.registerMapper,
        // dbi.registerArgumentFactory). This is because a transaction handle (Handle) created by a
        // DatabaseStoreManager-A could be used by another DatabaseStoreManager-B if A starts a transaction
        // and B runs a nested transaction (including autoCommit). B should not create another handle in this
        // case because creating a handle opens another connection. Opening a connection in a transaction causes
        // deadlock as following:
        //
        // * Thread1 starts a transaction and acquires a row lock (such as lockTaskIfExists).
        // * Thread2 opens a connection and try to lock the same row. This is blocked blocked because Thread1
        //   already locks the row.
        // * Thread1 tries to open another connection in the transaction. This could be blocked for ever by
        //   Thread2 if number of opened connection reached maximumPoolSize when thread2 opened a connection.
        //
        binder.bind(DBI.class).toProvider(DbiProvider.class).in(Scopes.SINGLETON);

        binder.bind(ConfigMapper.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseMigrator.class).in(Scopes.SINGLETON);
        binder.bind(ProjectStoreManager.class).to(DatabaseProjectStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(QueueSettingStoreManager.class).to(DatabaseQueueSettingStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(SessionStoreManager.class).to(DatabaseSessionStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleStoreManager.class).to(DatabaseScheduleStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseTaskQueueConfig.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseTaskQueueServer.class).in(Scopes.SINGLETON);
    }

    public static class AutoMigrator
    {
        private DatabaseMigrator migrator;

        @Inject
        public AutoMigrator(DataSource ds, DatabaseConfig config)
        {
            if (config.getAutoMigrate()) {
                this.migrator = new DatabaseMigrator(new DBI(ds), config);
            }
        }

        @PostConstruct
        public void migrate()
        {
            if (migrator != null) {
                migrator.migrate();
                migrator = null;
            }
        }
    }

    public static class DbiProvider
            implements Provider<DBI>
    {
        private final DataSource ds;

        @Inject
        // here depends on AutoMigrator so that @PostConstruct runs before StoreManager
        public DbiProvider(DataSource ds, AutoMigrator migrator)
        {
            this.ds = ds;
        }

        public DBI get()
        {
            return new DBI(ds);
        }
    }
}
