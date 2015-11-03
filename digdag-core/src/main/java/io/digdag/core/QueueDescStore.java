package io.digdag.core;

import java.util.List;
import com.google.common.base.*;
import io.digdag.core.config.Config;

public interface QueueDescStore
        extends Store
{
    List<StoredQueueDesc> getAllQueueDescs();  // TODO only for testing

    List<StoredQueueDesc> getQueueDescs(int pageSize, Optional<Long> lastId);

    StoredQueueDesc getQueueDescById(long qdId);

    StoredQueueDesc getQueueDescByName(String name);

    StoredQueueDesc getQueueDescByNameOrCreateDefault(String name, Config defaultConfig);

    void updateQueueDescConfig(long qdId, Config newConfig);
}
