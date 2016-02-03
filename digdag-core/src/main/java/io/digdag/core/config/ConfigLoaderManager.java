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
    private final HoconParameterizedConfigLoader hocon;

    @Inject
    public ConfigLoaderManager(ConfigFactory cf, YamlConfigLoader yaml, HoconParameterizedConfigLoader hocon)
    {
        this.cf = cf;
        this.yaml = yaml;
        this.hocon = hocon;
    }

    public Config loadParameterizedFile(File path, Config params)
        throws IOException
    {
        if (path.toString().endsWith(".conf")) {
            return hocon.loadParameterizedFile(path, params).toConfig(cf);
        }
        else {
            return yaml.loadParameterizedFile(path, params).toConfig(cf);
        }
    }
}
