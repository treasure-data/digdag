package io.digdag.core.queue;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public interface QueueSettingStore
{
    List<StoredQueueSetting> getQueueSettings(int pageSize, Optional<Long> lastId);

    StoredQueueSetting getQueueSettingById(long qdId)
        throws ResourceNotFoundException;

    StoredQueueSetting getQueueSettingByName(String name)
        throws ResourceNotFoundException;

    //// TODO remote agent and multiqueue are not implemented yet.
    // getQueuedTasks(Optional<Long> lastId)
    // getQueuedTasksOfQueue(int queueId, Optional<Long> lastId)
    // insertQueueSetting(QueueSetting, Runnable lockAction)
    // updateQueueSetting(int queueId, Config config, Runnable lockAction)
}
