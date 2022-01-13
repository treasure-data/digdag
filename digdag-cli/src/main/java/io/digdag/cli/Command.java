package io.digdag.cli;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.core.Environment;
import io.digdag.client.Version;
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
import java.io.StringReader;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class Command
{
    private static final Logger log = LoggerFactory.getLogger(Command.class);

    @Inject @Environment protected Map<String, String> env;
    @Inject protected Version version;
    @Inject @ProgramName protected String programName;
    @Inject @StdIn protected InputStream in;
    @Inject @StdOut protected PrintStream out;
    @Inject @StdErr protected PrintStream err;

    @Parameter()
    protected List<String> args = new ArrayList<>();

    @Parameter(names = {"-c", "--config"})
    protected String configPath = null;

    @Parameter(names = {"-L", "--log"})
    protected String logPath = "-";

    @Parameter(names = {"-l", "--log-level"})
    protected String logLevel = "info";

    @Parameter(names = {"--logback-config"})
    protected String logbackConfigPath = null;

    @DynamicParameter(names = "-X")
    protected Map<String, String> systemProperties = new HashMap<>();

    @Parameter(names = {"-help", "--help"}, help = true, hidden = true)
    protected boolean help;

    public abstract void main() throws Exception;

    public abstract SystemExitException usage(String error);

    protected Properties loadSystemProperties()
        throws IOException
    {
        // Property order of precedence:
        // 1. Explicit configuration file (if --config was specified)
        // 2. JVM System properties (-D...)
        // 3. DIGDAG_CONFIG env var
        // 4. Default config file (unless --config was specified)

        // This order was chosen as the most "natural" ordering, reflecting the order in which
        // users may supply configuration properties to digdag. Generally properties specified "later"
        // should take precedence.

        Properties props = new Properties();

        if (configPath == null) {
            // If no configuration file was specified, load the default configuration, if it exists.
            Path defaultConfigPath = ConfigUtil.defaultConfigPath(env);
            try {
                props.putAll(PropertyUtils.loadFile(defaultConfigPath));
            }
            catch (NoSuchFileException ex) {
                log.trace("configuration file not found: {}", defaultConfigPath, ex);
            }
        }

        // Load properties from DIGDAG_CONFIG env
        props.load(new StringReader(env.getOrDefault("DIGDAG_CONFIG", "")));

        // Load properties from system properties
        props.putAll(System.getProperties());

        // Load explicit configuration file, if specified.
        if (configPath != null) {
            props.putAll(PropertyUtils.loadFile(Paths.get(configPath)));
        }

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
            localRepositoryPath = ConfigUtil.defaultLocalPluginPath(env);
        }
        else {
            localRepositoryPath = Paths.get(localPath);
        }
        PluginSet remotePlugins = new RemotePluginLoader(localRepositoryPath).load(spec);

        return localPlugins.withPlugins(remotePlugins);
    }
}
