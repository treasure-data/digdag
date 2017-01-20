package io.digdag.core.database;

import com.google.inject.Inject;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.sun.tools.javac.util.Assert.checkNull;

public class ThreadLocalTransactionManager
        implements TransactionManager
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);
    private static final ThreadLocal<Transaction> threadLocalTransaction = new ThreadLocal<>();
    private final DataSource ds;
    private final ConfigMapper configMapper;

    private static class LazyTransaction
            implements Transaction
    {
        private final DataSource ds;
        private final ConfigMapper configMapper;
        private Handle handle;

        LazyTransaction(DataSource ds, ConfigMapper configMapper)
        {
            this.ds = checkNotNull(ds);
            this.configMapper = configMapper;
        }

        @Override
        public Handle getHandle(ConfigMapper configMapper)
        {
            if (handle == null) {
                DBI dbi = new DBI(ds);
                ConfigKeyListMapper cklm = new ConfigKeyListMapper();
                dbi.registerMapper(new DatabaseProjectStoreManager.StoredProjectMapper(configMapper));
                dbi.registerMapper(new DatabaseProjectStoreManager.StoredRevisionMapper(configMapper));
                dbi.registerMapper(new DatabaseProjectStoreManager.StoredWorkflowDefinitionMapper(configMapper));
                dbi.registerMapper(new DatabaseProjectStoreManager.StoredWorkflowDefinitionWithProjectMapper(configMapper));
                dbi.registerMapper(new DatabaseProjectStoreManager.WorkflowConfigMapper());
                dbi.registerMapper(new DatabaseProjectStoreManager.IdNameMapper());
                dbi.registerMapper(new DatabaseQueueSettingStoreManager.StoredQueueSettingMapper(configMapper));
                dbi.registerMapper(new DatabaseScheduleStoreManager.StoredScheduleMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredTaskMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.ArchivedTaskMapper(cklm, configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.ResumingTaskMapper(cklm, configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredSessionMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredSessionWithLastAttemptMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredSessionAttemptMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredSessionAttemptWithSessionMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.TaskStateSummaryMapper());
                dbi.registerMapper(new DatabaseSessionStoreManager.TaskAttemptSummaryMapper());
                dbi.registerMapper(new DatabaseSessionStoreManager.SessionAttemptSummaryMapper());
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredSessionMonitorMapper(configMapper));
                dbi.registerMapper(new DatabaseSessionStoreManager.StoredDelayedSessionAttemptMapper());
                dbi.registerMapper(new DatabaseSessionStoreManager.TaskRelationMapper());
                dbi.registerMapper(new DatabaseSessionStoreManager.InstantMapper());
                dbi.registerMapper(new DatabaseSecretStore.ScopedSecretMapper());
                dbi.registerMapper(new DatabaseTaskQueueServer.ImmutableTaskQueueLockMapper());

                dbi.registerArgumentFactory(configMapper.getArgumentFactory());
                handle = dbi.open();
            }
            return handle;
        }

        @Override
        public void commit()
        {
            if (handle == null) {
                logger.warn("Committing without any database operation");
                return;
            }
            handle.commit();
        }

        @Override
        public void abort()
        {
            if (handle == null) {
                return;
            }
            handle.rollback();
            // TODO Should call close()?
        }
    }

    @Inject
    public ThreadLocalTransactionManager(DataSource ds, ConfigMapper configMapper)
    {
        this.ds = checkNotNull(ds);
        this.configMapper = checkNotNull(configMapper);
    }

    @Override
    public Handle getHandle(ConfigMapper configMapper)
    {
        Transaction transaction = checkNotNull(threadLocalTransaction.get());
        return transaction.getHandle(configMapper);
    }

    @Override
    public <T> T begin(Supplier<T> func)
    {
        checkNull(threadLocalTransaction.get());
        LazyTransaction transaction = new LazyTransaction(ds, configMapper);
        threadLocalTransaction.set(transaction);
        boolean committed = false;
        try {
            T result = func.get();
            transaction.commit();
            committed = true;
            return result;
        }
        finally {
            if (!committed) {
                transaction.abort();
            }
        }
    }
}
