package io.digdag.core.database;

import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.inject.Provider;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.AgentId;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.workflow.TaskQueueDispatcher;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.Notifier;
import io.digdag.spi.TaskQueueRequest;
import java.util.Base64;
import java.util.Random;
import org.skife.jdbi.v2.DBI;

import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigMapper;
import static org.mockito.Mockito.mock;

public class DatabaseFactory
        implements AutoCloseable, Provider<DBI>
{
    private final DBI dbi;
    private final AutoCloseable closeable;
    private final DatabaseConfig config;

    public DatabaseFactory(DBI dbi, AutoCloseable closeable, DatabaseConfig config)
    {
        this.dbi = dbi;
        this.closeable = closeable;
        this.config = config;
    }

    @Override
    public DBI get()
    {
        return dbi;
    }

    public DatabaseConfig getConfig()
    {
        return config;
    }

    public DatabaseProjectStoreManager getProjectStoreManager()
    {
        return getProjectStoreManager(newRandomSecretCrypto());
    }

    public DatabaseProjectStoreManager getProjectStoreManager(SecretCrypto sharedSecret)
    {
        return new DatabaseProjectStoreManager(dbi, createConfigMapper(), config, sharedSecret);
    }

    public SecretCrypto newRandomSecretCrypto()
    {
        byte[] random = new byte[16];
        new Random().nextBytes(random);
        String sharedSecret = Base64.getEncoder().encodeToString(random);
        return new AESGCMSecretCrypto(sharedSecret);
    }

    public DatabaseScheduleStoreManager getScheduleStoreManager()
    {
        return new DatabaseScheduleStoreManager(dbi, createConfigMapper(), config);
    }

    public DatabaseSessionStoreManager getSessionStoreManager()
    {
        return new DatabaseSessionStoreManager(dbi, createConfigFactory(), createConfigMapper(), objectMapper(), config);
    }

    public WorkflowExecutor getWorkflowExecutor()
    {
        ConfigFactory configFactory = createConfigFactory();
        return new WorkflowExecutor(
                getProjectStoreManager(),
                getSessionStoreManager(),
                new NullTaskQueueDispatcher(),
                new WorkflowCompiler(),
                configFactory,
                objectMapper(),
                configFactory.create(),
                mock(Notifier.class));
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
            throw Throwables.propagate(ex);
        }
    }
}
