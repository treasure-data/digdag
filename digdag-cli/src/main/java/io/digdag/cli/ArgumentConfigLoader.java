package io.digdag.cli;

import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.yaml.YamlConfigLoader;

public class ArgumentConfigLoader
{
    private final YamlConfigLoader loader;
    private final ConfigFactory cf;

    @Inject
    public ArgumentConfigLoader(YamlConfigLoader loader, ConfigFactory cf)
    {
        this.loader = loader;
        this.cf = cf;
    }

    public boolean checkExists(File file)
    {
        return file.exists() && file.length() > 0;
    }

    public Config load(File path, Config params)
        throws IOException
    {
        // TODO use yaml if file path ends with yml, otherwise use json?
        return loader.loadFile(
                path,
                Optional.of(
                    new File(new File(path.getAbsolutePath()).getParent())
                ),
                Optional.of(cf.create()));
    }
}
