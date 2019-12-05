package io.digdag.spi;

import io.digdag.client.config.Config;

public interface StorageFactory
{
    String getType();

    Storage newStorage(Config config);
}
