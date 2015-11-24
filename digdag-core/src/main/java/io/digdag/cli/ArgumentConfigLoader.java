package io.digdag.cli;

import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;

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

    public Config load(File path)
        throws IOException
    {
        return loader.loadFile(
                path,
                Optional.of(
                    new File(new File(path.getAbsolutePath()).getParent())
                ),
                cf.create());
    }
}
