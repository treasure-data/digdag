package io.digdag.cli;

import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.yaml.YamlConfigLoader;

public class ArgumentConfigLoader
{
    private final YamlConfigLoader loader;

    @Inject
    public ArgumentConfigLoader(YamlConfigLoader loader)
    {
        this.loader = loader;
    }

    public boolean checkExists(File file)
    {
        return file.exists() && file.length() > 0;
    }

    public Config load(File path, Config params)
        throws IOException
    {
        return loader.loadFile(path, Optional.of(new File(path.getAbsolutePath())), Optional.of(params));
    }
}
