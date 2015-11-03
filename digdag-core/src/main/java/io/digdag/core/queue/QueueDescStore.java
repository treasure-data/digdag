package io.digdag.core.queue;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.config.Config;

public interface QueueDescStore
{
    List<StoredQueueDesc> getAllQueueDescs();  // TODO only for testing

    List<StoredQueueDesc> getQueueDescs(int pageSize, Optional<Long> lastId);

    StoredQueueDesc getQueueDescById(long qdId);

    StoredQueueDesc getQueueDescByName(String name);

    StoredQueueDesc getQueueDescByNameOrCreateDefault(String name, Config defaultConfig);

    void updateQueueDescConfig(long qdId, Config newConfig);
}
