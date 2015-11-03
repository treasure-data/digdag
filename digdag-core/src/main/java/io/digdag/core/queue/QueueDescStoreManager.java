package io.digdag.core.queue;

public interface QueueDescStoreManager
{
    QueueDescStore getQueueDescStore(int siteId);
}
