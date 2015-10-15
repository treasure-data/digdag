package io.digdag.core;

import java.util.List;
import com.google.common.base.*;

public interface QueueDescStore
        extends Store
{
    List<StoredQueueDesc> getAllQueueDescs();  // TODO only for testing

    List<StoredQueueDesc> getQueueDescs(int pageSize, Optional<Long> lastId);

    StoredQueueDesc getQueueDescById(long qdId);

    StoredQueueDesc getQueueDescByName(String name);

    StoredQueueDesc getQueueDescOrCreateDefault(String name, ConfigSource defaultConfig);

    void updateQueueDescConfig(long qdId, ConfigSource newConfig);
}
