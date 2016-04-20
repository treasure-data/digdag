package io.digdag.cli;

import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import io.digdag.core.config.PropertyUtils;
import io.digdag.server.ServerBootstrap;
import static io.digdag.cli.Main.systemExit;
import static io.digdag.server.ServerConfig.DEFAULT_PORT;
import static io.digdag.server.ServerConfig.DEFAULT_BIND;

public class Server
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

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

    @Parameter(names = {"-c", "--config"})
    String configPath = null;

    @Override
    public void main()
            throws Exception
    {
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
        System.err.println("Usage: digdag server [options...]");
        System.err.println("  Options:");
        System.err.println("    -n, --port PORT                  port number to listen for web interface and api clients (default: " + DEFAULT_PORT + ")");
        System.err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: " + DEFAULT_BIND + ")");
        System.err.println("    -m, --memory                     uses memory database");
        System.err.println("    -o, --database DIR               store status to this database");
        System.err.println("    -O, --task-log DIR               store task logs to this database");
        System.err.println("    -A, --access-log DIR             store access logs files to this path");
        System.err.println("    -c, --config PATH.properties     server configuration property path");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void server()
            throws ServletException, IOException
    {
        ServerBootstrap.startServer(buildServerProperties(), ServerBootstrap.class);
    }

    protected Properties buildServerProperties()
        throws IOException
    {
        // parameters for ServerBootstrap
        Properties props = loadSystemProperties();

        if (configPath != null) {
            props.putAll(PropertyUtils.loadFile(new File(configPath)));
        }

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

        return props;
    }
}
