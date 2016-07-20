package io.digdag.core.database;

import io.digdag.client.config.Config;
import com.google.inject.Inject;

public class DatabaseTaskQueueConfig
{
    private final int defaultMaxConcurrency;

    @Inject
    public DatabaseTaskQueueConfig(Config systemConfig)
    {
        this.defaultMaxConcurrency = systemConfig.get("queue.db.max_concurrency", int.class, Integer.MAX_VALUE);
    }

    public int getSiteMaxConcurrency(int siteId)
    {
        return defaultMaxConcurrency;
    }
}
