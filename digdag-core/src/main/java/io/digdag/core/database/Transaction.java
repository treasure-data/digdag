package io.digdag.core.database;

import org.skife.jdbi.v2.Handle;

public interface Transaction
{
    Handle getHandle(ConfigMapper configMapper);

    void commit();

    void abort();
}
