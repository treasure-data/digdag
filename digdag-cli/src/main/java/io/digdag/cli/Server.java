package io.digdag.cli;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.io.IOException;
import java.nio.file.Paths;
import javax.servlet.ServletException;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Version;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.server.ServerBootstrap;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.server.ServerConfig.DEFAULT_PORT;
import static io.digdag.server.ServerConfig.DEFAULT_BIND;

public class Server
    extends Command
{
    @Parameter(names = {"-n", "--port"})
    Integer port = null;

    @Parameter(names = {"-b", "--bind"})
    String bind = null;

    @Parameter(names = {"-m", "--memory"})
    boolean memoryDatabase = false;

    @Parameter(names = {"-o", "--database"})
    String database = null;

    @Parameter(names = {"-O", "--task-log"})
    String taskLogPath = null;

    @Parameter(names = {"-A", "--access-log"})
    String accessLogPath = null;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    protected final Version localVersion;

    public Server(Version localVersion, PrintStream out, PrintStream err)
    {
        super(out, err);
        this.localVersion = localVersion;
    }

    @Override
    public void main()
            throws Exception
    {
        JvmUtil.validateJavaRuntime(err);

        if (args.size() != 0) {
            throw usage(null);
        }

        if (database == null && memoryDatabase == false && configPath == null) {
            throw usage("--database, --memory, or --config option is required");
        }
        if (database != null && memoryDatabase == true) {
            throw usage("Setting both --database and --memory is invalid");
        }

        server();
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag server [options...]");
        err.println("  Options:");
        err.println("    -n, --port PORT                  port number to listen for web interface and api clients (default: " + DEFAULT_PORT + ")");
        err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: " + DEFAULT_BIND + ")");
        err.println("    -m, --memory                     uses memory database");
        err.println("    -o, --database DIR               store status to this database");
        err.println("    -O, --task-log DIR               store task logs to this database");
        err.println("    -A, --access-log DIR             store access logs files to this path");
        err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        err.println("    -c, --config PATH.properties     server configuration property path");
        Main.showCommonOptions(err);
        return systemExit(error);
    }

    private void server()
            throws ServletException, IOException
    {
        ServerBootstrap.startServer(localVersion, buildServerProperties(), ServerBootstrap.class);
    }

    protected Properties buildServerProperties()
        throws IOException
    {
        // parameters for ServerBootstrap
        Properties props = loadSystemProperties();

        // overwrite by command-line parameters
        if (database != null) {
            props.setProperty("database.type", "h2");
            props.setProperty("database.path", Paths.get(database).toAbsolutePath().toString());
        }
        else if (memoryDatabase) {
            props.setProperty("database.type", "memory");
        }

        if (port != null) {
            props.setProperty("server.port", Integer.toString(port));
        }

        if (bind != null) {
            props.setProperty("server.bind", bind);
        }

        if (taskLogPath != null) {
            props.setProperty("log-server.type", "local");
            props.setProperty("log-server.local.path", taskLogPath);
        }

        if (accessLogPath != null) {
            props.setProperty("server.access-log.path", accessLogPath);
        }

        // Load default parameters
        ConfigFactory cf = new ConfigFactory(objectMapper());
        Config defaultParams = loadParams(
                cf, new ConfigLoaderManager(cf, new YamlConfigLoader()),
                props, paramsFile, params);

        props.setProperty("digdag.defaultParams", defaultParams.toString());

        return props;
    }
}
