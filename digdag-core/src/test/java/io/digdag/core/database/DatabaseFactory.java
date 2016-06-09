package io.digdag.core.database;

import com.google.common.base.Throwables;
import com.google.inject.Provider;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.AgentId;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.workflow.TaskQueueDispatcher;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.Notifier;
import io.digdag.spi.TaskRequest;
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

    public DBI get()
    {
        return dbi;
    }

    public DatabaseProjectStoreManager getProjectStoreManager()
    {
        return new DatabaseProjectStoreManager(dbi, createConfigMapper(), config);
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
            extends TaskQueueDispatcher
    {
        public NullTaskQueueDispatcher()
        {
            super(null);
        }

        @Override
        public void dispatch(TaskRequest request)
            throws ResourceConflictException
        { }

        @Override
        public void taskFinished(int siteId, String lockId, AgentId agentId)
        { }
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
