package io.digdag.cli;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import io.digdag.client.Version;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.server.ServerBootstrap;
import io.digdag.server.ServerConfig;

import javax.servlet.ServletException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.server.ServerConfig.DEFAULT_BIND;
import static io.digdag.server.ServerConfig.DEFAULT_PORT;

public class Server
    extends Command
{
    @Parameter(names = {"-n", "--port"})
    Integer port = null;

    @Parameter(names = {"-b", "--bind"})
    String bind = null;

    @Parameter(names = {"--admin-port"})
    Integer adminPort = null;

    @Parameter(names = {"--admin-bind"})
    String adminBind = null;

    @Parameter(names = {"-m", "--memory"})
    boolean memoryDatabase = false;

    @Parameter(names = {"-o", "--database"})
    String database = null;

    @Parameter(names = {"-O", "--task-log"})
    String taskLogPath = null;

    @Parameter(names = {"-A", "--access-log"})
    String accessLogPath = null;

    @Parameter(names = {"--disable-local-agent"})
    boolean disableLocalAgent = false;

    @Parameter(names = {"--max-task-threads"})
    Integer maxTaskThreads = null;

    @Parameter(names = {"--disable-executor-loop"})
    boolean disableExecutorLoop = false;

    @Parameter(names = {"--disable-scheduler"})
    boolean disableScheduler = false;

    @Parameter(names = {"--enable-swagger"})
    boolean enableSwagger = false;

    @Parameter(names = {"-p", "--param"}, validateWith = ParameterValidator.class)
    List<String> paramsList = new ArrayList<>();
    Map<String, String> params = new HashMap<>();

    @Parameter(names= {"-H", "--header"}, validateWith = ParameterValidator.class)
    List<String> headersList = new ArrayList<>();
    Map<String, String> headers = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

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

        startServer();
    }

    @Override
    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " server [options...]");
        err.println("  Options:");
        err.println("    -n, --port PORT                  port number to listen for web interface and api clients (default: " + DEFAULT_PORT + ")");
        err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: " + DEFAULT_BIND + ")");
        err.println("    --admin-port PORT                port number to bind admin api on (default: no admin port)");
        err.println("    --admin-bind ADDRESS             IP address to bind admin api on (default: same address with --bind)");
        err.println("    -m, --memory                     uses memory database");
        err.println("    -o, --database DIR               store status to this database");
        err.println("    -O, --task-log DIR               store task logs to this path");
        err.println("    -A, --access-log DIR             store access logs files to this path");
        err.println("        --max-task-threads N         limit maximum number of task execution threads");
        err.println("        --disable-executor-loop      disable workflow executor loop");
        err.println("        --disable-scheduler          disable scheduler");
        err.println("        --disable-local-agent        disable local task execution");
        err.println("        --enable-swagger             enable swagger api");
        err.println("    -p, --param KEY=VALUE            overwrites a parameter (use multiple times to set many parameters)");
        err.println("    -H, --header KEY=VALUE           a header to include in api HTTP responses");
        err.println("    -P, --params-file PATH.yml       reads parameters from a YAML file");
        err.println("    -c, --config PATH.properties     server configuration property path");
        Main.showCommonOptions(env, err);
        return systemExit(error);
    }

    private void startServer()
            throws ServletException, IOException
    {
        // this method doesn't block. it starts some non-daemon threads, setup shutdown handlers, and returns immediately
        Properties props = buildServerProperties();
        ConfigElement ce = PropertyUtils.toConfigElement(props);
        ServerConfig serverConfig = ServerConfig.convertFrom(ce);
        ServerBootstrap.start(buildServerBootstrap(version, serverConfig));
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

        if (adminPort != null) {
            props.setProperty("server.admin.port", Integer.toString(adminPort));
        }

        if (adminBind != null) {
            props.setProperty("server.admin.bind", adminBind);
        }

        if (taskLogPath != null) {
            props.setProperty("log-server.type", "local");
            props.setProperty("log-server.local.path", taskLogPath);
        }

        if (accessLogPath != null) {
            props.setProperty("server.access-log.path", accessLogPath);
        }

        if (disableLocalAgent) {
            props.setProperty("agent.enabled", Boolean.toString(false));
        }

        if (maxTaskThreads != null) {
            props.setProperty("agent.max-task-threads", Integer.toString(maxTaskThreads));
        }

        if (disableExecutorLoop) {
            props.setProperty("server.executor.enabled", Boolean.toString(false));
        }

        if (disableScheduler) {
            props.setProperty("schedule.enabled", Boolean.toString(false));
        }

        if (enableSwagger) {
            props.setProperty("server.enable-swagger", Boolean.toString(true));
        }

        headers = ParameterValidator.toMap(headersList);
        headers.forEach((key, value) -> props.setProperty("server.http.headers." + key, value));

        // Load default parameters
        ConfigFactory cf = new ConfigFactory(objectMapper());
        params = ParameterValidator.toMap(paramsList);
        Config defaultParams = loadParams(
                cf, new ConfigLoaderManager(cf, new YamlConfigLoader()),
                props, paramsFile, params);

        props.setProperty("digdag.defaultParams", defaultParams.toString());

        env.forEach((key, value) -> props.setProperty("server.environment." + key, value));

        return props;
    }

    protected ServerBootstrap buildServerBootstrap(final Version version, final ServerConfig serverConfig)
    {
        return new ServerBootstrap(version, serverConfig);
    }
}
