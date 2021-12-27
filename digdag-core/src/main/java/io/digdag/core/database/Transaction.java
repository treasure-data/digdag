package io.digdag.core.database;

import org.jdbi.v3.core.Handle;

public interface Transaction
{
    Handle getHandle(ConfigMapper configMapper);

    void commit();

    void abort();

    void reset();
}
