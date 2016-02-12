package io.digdag.core.queue;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public interface QueueSettingStore
{
    static final String DEFAULT_QUEUE_NAME = "default";
    static final int NO_MAX_CONCURRENCY = Integer.MAX_VALUE;

    List<StoredQueueSetting> getQueueSettings(int pageSize, Optional<Long> lastId);

    StoredQueueSetting getQueueSettingById(long qdId)
        throws ResourceNotFoundException;

    StoredQueueSetting getQueueSettingByName(String name)
        throws ResourceNotFoundException;
}
