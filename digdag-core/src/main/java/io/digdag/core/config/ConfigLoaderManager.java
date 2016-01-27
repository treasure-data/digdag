package io.digdag.core.config;

import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class ConfigLoaderManager
{
    private final YamlConfigLoader loader;

    @Inject
    public ConfigLoaderManager(YamlConfigLoader loader)
    {
        this.loader = loader;
    }

    public Config loadParameterizedFile(File path, Config params)
        throws IOException
    {
        return loader.loadFile(path, Optional.of(new File(path.getAbsolutePath())), Optional.of(params));
    }
}
