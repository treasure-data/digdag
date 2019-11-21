package io.digdag.spi;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public interface StorageFactory
{
    String getType();

    Storage newStorage(Config config) throws ConfigException;
}
