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
    private boolean withTaskQueueServer;

    public DatabaseModule(boolean withTaskQueueServer)
    {
        this.withTaskQueueServer = withTaskQueueServer;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DatabaseConfig.class).toProvider(DatabaseConfigProvider.class).in(Scopes.SINGLETON);
        binder.bind(DataSource.class).toProvider(DataSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(AutoMigrator.class);
        binder.bind(DBI.class).toProvider(DbiProvider.class);  // don't make this singleton because DBI.registerMapper is called for each StoreManager
        binder.bind(TransactionManager.class).to(ThreadLocalTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ConfigMapper.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseMigrator.class).in(Scopes.SINGLETON);
        binder.bind(ProjectStoreManager.class).to(DatabaseProjectStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(QueueSettingStoreManager.class).to(DatabaseQueueSettingStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(SessionStoreManager.class).to(DatabaseSessionStoreManager.class).in(Scopes.SINGLETON);
        binder.bind(ScheduleStoreManager.class).to(DatabaseScheduleStoreManager.class).in(Scopes.SINGLETON);
        if (withTaskQueueServer) {
            binder.bind(DatabaseTaskQueueConfig.class).in(Scopes.SINGLETON);
            binder.bind(DatabaseTaskQueueServer.class).in(Scopes.SINGLETON);
        }
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

        @Override
        public DBI get()
        {
            return new DBI(ds);
        }
    }
}
