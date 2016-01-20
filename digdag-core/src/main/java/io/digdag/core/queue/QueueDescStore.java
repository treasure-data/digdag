package io.digdag.core.queue;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public interface QueueDescStore
{
    List<StoredQueueDesc> getQueueDescs(int pageSize, Optional<Long> lastId);

    StoredQueueDesc getQueueDescById(long qdId)
        throws ResourceNotFoundException;

    StoredQueueDesc getQueueDescByName(String name)
        throws ResourceNotFoundException;

    StoredQueueDesc getQueueDescByNameOrCreateDefault(String name, Config defaultConfig);

    void updateQueueDescConfig(long qdId, Config newConfig);
}
