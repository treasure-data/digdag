package io.digdag.core.config;

import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class ConfigLoaderManager
{
    private final ConfigFactory cf;
    private final YamlConfigLoader yaml;

    @Inject
    public ConfigLoaderManager(ConfigFactory cf, YamlConfigLoader yaml)
    {
        this.cf = cf;
        this.yaml = yaml;
    }

    public Config loadParameterizedFile(File path, Config params)
        throws IOException
    {
        // TODO check suffix .dig, .yml or .yaml. otherwise throw exception
        return yaml.loadParameterizedFile(path, params).toConfig(cf);
    }
}
