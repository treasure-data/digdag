package io.digdag.core.database;

import io.digdag.client.config.Config;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.core.database.DatabaseTestingUtils.createConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.setupDatabase;

public class DatabaseQueueTest
{
    private DatabaseFactory factory;
    private DatabaseTaskQueueServer taskQueue;

    @Before
    public void setUp()
        throws Exception
    {
        factory = setupDatabase();
        Config systemConfig = createConfigFactory()
            .create()
            .set("queue.db.max_concurrency", 2);
        taskQueue = new DatabaseTaskQueueServer(
                factory.get(),
                factory.getConfig(),
                new DatabaseTaskQueueConfig(systemConfig),
                objectMapper());
    }

    @Test
    public void testConcurrency()
    {
        // TODO
    }
}
