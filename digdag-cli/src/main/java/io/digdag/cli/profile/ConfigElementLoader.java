package io.digdag.cli.profile;

import com.google.common.collect.ImmutableMap;
import io.digdag.cli.Command;
import io.digdag.cli.SystemExitException;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.config.PropertyUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;


public class ConfigElementLoader
{
    private final File configFile;

    public ConfigElementLoader(File configFile)
    {
        checkNotNull(configFile);
        this.configFile = configFile;
    }

    public ConfigElement load()
            throws IOException
    {
        return new CommandWrapper(configFile).loadConfigElement();
    }

    private class CommandWrapper
        extends Command
    {
        CommandWrapper(File configFile)
        {
            super();
            this.env = ImmutableMap.of();
            this.configPath = configFile.getAbsolutePath();
        }

        @Override
        public void main()
                throws Exception
        {
            throw new IllegalStateException("This method shouldn't be called");
        }

        @Override
        public SystemExitException usage(String error)
        {
            throw new IllegalStateException("This method shouldn't be called");
        }

        private ConfigElement loadConfigElement()
                throws IOException
        {
            Properties props = loadSystemProperties();
            return PropertyUtils.toConfigElement(props);
        }
    }
}
