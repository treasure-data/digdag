package io.digdag.core.queue;

import io.digdag.core.repository.ResourceNotFoundException;

public interface QueueSettingStoreManager
{
    QueueSettingStore getQueueSettingStore(int siteId);

    int getQueueIdByName(int siteId, String name)
        throws ResourceNotFoundException;
}
