package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.core.Limits;
import io.digdag.core.agent.AgentId;
import io.digdag.core.workflow.TaskQueueDispatcher;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.spi.TaskQueueRequest;

import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigMapper;

public class DatabaseFactory
        implements AutoCloseable, Provider<TransactionManager>
{
    private final TransactionManager tm;
    private final AutoCloseable closeable;
    private final DatabaseConfig config;

    public DatabaseFactory(TransactionManager tm, AutoCloseable closeable, DatabaseConfig config)
    {
        this.tm = tm;
        this.closeable = closeable;
        this.config = config;
    }

    @Override
    public TransactionManager get()
    {
        return tm;
    }

    public <T> T begin(TransactionManager.SupplierInTransaction<T, Exception, RuntimeException, RuntimeException, RuntimeException> func)
            throws Exception
    {
        return tm.begin(func, Exception.class);
    }

    public void begin(ThrowableRunnable func)
            throws Exception
    {
        begin(() -> {
            func.run();
            return null;
        });
    }

    @FunctionalInterface
    public interface ThrowableRunnable
    {
        void run() throws Exception;
    }

    public <T> T autoCommit(TransactionManager.SupplierInTransaction<T, Exception, RuntimeException, RuntimeException, RuntimeException> func)
            throws Exception
    {
        return tm.autoCommit(func, Exception.class);
    }

    public void autoCommit(ThrowableRunnable func)
            throws Exception
    {
        autoCommit(() -> {
            func.run();
            return null;
        });
    }

    public DatabaseConfig getConfig()
    {
        return config;
    }

    public DatabaseProjectStoreManager getProjectStoreManager()
    {
        return new DatabaseProjectStoreManager(tm, createConfigMapper(), config);
    }

    public DatabaseScheduleStoreManager getScheduleStoreManager()
    {
        return new DatabaseScheduleStoreManager(tm, createConfigMapper(), config);
    }

    public DatabaseSessionStoreManager getSessionStoreManager()
    {
        return new DatabaseSessionStoreManager(createConfigFactory(), tm, createConfigMapper(), objectMapper(), config);
    }

    public WorkflowExecutor getWorkflowExecutor()
    {
        ConfigFactory configFactory = createConfigFactory();
        Config systemConfig = configFactory.create();
        return new WorkflowExecutor(
                getProjectStoreManager(),
                getSessionStoreManager(),
                tm,
                new NullTaskQueueDispatcher(),
                new WorkflowCompiler(),
                configFactory,
                objectMapper(),
                systemConfig,
                new Limits(systemConfig),
                StdDigdagMetrics.empty()
        );
    }

    public DatabaseSecretControlStoreManager getSecretControlStoreManager(String secret)
    {
        return new DatabaseSecretControlStoreManager(config, tm, createConfigMapper(), new AESGCMSecretCrypto(secret));
    }

    public DatabaseSecretStoreManager getSecretStoreManager(String secret)
    {
        return new DatabaseSecretStoreManager(config, tm, createConfigMapper(), new AESGCMSecretCrypto(secret));
    }

    public static class NullTaskQueueDispatcher
            implements TaskQueueDispatcher
    {
        @Override
        public void dispatch(int siteId, Optional<String> queueName, TaskQueueRequest request)
        { }

        @Override
        public void taskFinished(int siteId, String lockId, AgentId agentId)
        { }

        @Override
        public boolean deleteInconsistentTask(String lockId)
        {
            return false;
        }
    }

    public void close()
    {
        try {
            closeable.close();
        }
        catch (Exception ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }
}
