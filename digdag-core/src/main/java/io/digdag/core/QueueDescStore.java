package io.digdag.core;

public interface QueueDescStore
        extends Store
{
    Pageable<QueueDesc> getQueueDescs();

    QueueDesc getQueueDescById(int qdId);

    QueueDesc getQueueDescByName(String name);
}
