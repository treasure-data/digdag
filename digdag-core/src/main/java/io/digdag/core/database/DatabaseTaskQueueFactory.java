package io.digdag.core.database;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskQueueFactory;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskRequest;

public class DatabaseTaskQueueFactory
    implements TaskQueueFactory
{
    private final DatabaseTaskQueueServer database;
    private final Object sharedTaskSleepHelper = new Object();
    private final Object taskSleepHelper = new Object();

    @Inject
    public DatabaseTaskQueueFactory(DatabaseTaskQueueServer database)
    {
        this.database = database;
    }

    @Override
    public String getType()
    {
        return "database";
    }

    @Override
    public TaskQueueServer newServer(Config systemConfig)
    {
        return database;
    }

    @Override
    public TaskQueueClient newDirectClient(Config systemConfig)
    {
        return database;
    }
}
