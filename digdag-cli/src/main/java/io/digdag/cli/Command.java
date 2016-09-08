package io.digdag.cli;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.core.Environment;
import io.digdag.core.Version;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.plugin.PluginSet;
import io.digdag.core.plugin.LocalPluginLoader;
import io.digdag.core.plugin.RemotePluginLoader;
import io.digdag.core.plugin.Spec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public abstract class Command
{
    private static final Logger log = LoggerFactory.getLogger(Command.class);

    protected final CommandContext ctx;

    @Parameter()
    protected List<String> args = new ArrayList<>();

    @Parameter(names = {"-c", "--config"})
    protected String configPath = null;

    @Parameter(names = {"-L", "--log"})
    protected String logPath = "-";

    @Parameter(names = {"-l", "--log-level"})
    protected String logLevel = "info";

    @DynamicParameter(names = "-X")
    protected Map<String, String> systemProperties = new HashMap<>();

    @Parameter(names = {"-help", "--help"}, help = true, hidden = true)
    protected boolean help;

    public Command(CommandContext ctx)
    {
        this.ctx = Objects.requireNonNull(ctx);
    }

    public abstract void main() throws Exception;

    public abstract SystemExitException usage(String error);

    protected Properties loadSystemProperties()
        throws IOException
    {
        Properties props;

        // Load specific configuration file, if specified.
        if (configPath != null) {
            props = PropertyUtils.loadFile(Paths.get(configPath));
        } else {
            // If no configuration file was specified, load the default configuration, if it exists.
            try {
                props = PropertyUtils.loadFile(ConfigUtil.defaultConfigPath(ctx.environment()));
            }
            catch (NoSuchFileException ex) {
                log.trace("configuration file not found: {}", configPath, ex);
                props = new Properties();
            }
        }

        // Override properties from config file with system properties
        props.putAll(System.getProperties());

        return props;
    }

    protected PluginSet loadSystemPlugins(Properties systemProps)
    {
        // load plugins in classpath
        PluginSet localPlugins = new LocalPluginLoader().load(Command.class.getClassLoader());

        // load plugins from remote repositories set at configuration
        Spec spec = Spec.of(
                ImmutableList.copyOf(PropertyUtils.toMap(systemProps, "system-plugin.repositories").values()),
                ImmutableList.copyOf(PropertyUtils.toMap(systemProps, "system-plugin.dependencies").values()));
        String localPath = systemProps.getProperty("system-plugin.local-path", "");
        Path localRepositoryPath;
        if (localPath.equals("")) {
            localRepositoryPath = ConfigUtil.defaultLocalPluginPath(ctx.environment());
        }
        else {
            localRepositoryPath = Paths.get(localPath);
        }
        PluginSet remotePlugins = new RemotePluginLoader(localRepositoryPath).load(spec);

        return localPlugins.withPlugins(remotePlugins);
    }
}
