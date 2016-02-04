package io.digdag.cli;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.nio.file.FileSystems;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;
import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.guice.rs.GuiceRsServerControlModule;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.config.PropertyUtils;
import io.digdag.server.ServerConfig;
import static io.digdag.cli.Main.systemExit;
import static io.digdag.server.ServerConfig.DEFAULT_PORT;
import static io.digdag.server.ServerConfig.DEFAULT_BIND;

public class Server
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    @Parameter(names = {"-t", "--port"})
    Integer port = null;

    @Parameter(names = {"-b", "--bind"})
    String bind = null;

    @Parameter(names = {"-o", "--database"})
    String database = null;

    @Parameter(names = {"-m", "--memory"})
    boolean memoryDatabase = false;

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

        server();
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag server [options...]");
        System.err.println("  Options:");
        System.err.println("    -t, --port PORT                  port number to listen for web interface and api clients (default: " + DEFAULT_PORT + ")");
        System.err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: " + DEFAULT_BIND + ")");
        System.err.println("    -o, --database DIR               store status to this database");
        System.err.println("    -m, --memory                     uses memory database");
        System.err.println("    -c, --config PATH.properties     server configuration property path");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void server()
            throws ServletException, IOException
    {
        startServer(null);
    }

    protected void startServer(String autoloadLocalDagFile)
            throws ServletException, IOException
    {
        // parameters for ServerBootstrap
        Properties props = Main.loadProperties(configPath);

        // 3. overwrite by command-line parameters
        if (database != null) {
            props.setProperty("database.type", "h2");
            props.setProperty("database.path", FileSystems.getDefault().getPath(database).toAbsolutePath().toString());
        }
        if (port != null) {
            props.setProperty("server.port", Integer.toString(port));
        }
        if (bind != null) {
            props.setProperty("server.bind", bind);
        }
        if (autoloadLocalDagFile != null) {
            props.setProperty("server.autoLoadLocalDagfile", autoloadLocalDagFile);
        }

        // convert params to ConfigElement used for DatabaseConfig and other configs
        ConfigElement ce = PropertyUtils.toConfigElement(props);
        ServerConfig config = ServerConfig.convertFrom(ce);

        DeploymentInfo servletBuilder = Servlets.deployment()
            .setClassLoader(Main.class.getClassLoader())
            .setContextPath("/digdag/server")
            .setDeploymentName("digdag-server.war")
            .addServletContainerInitalizer(
                    new ServletContainerInitializerInfo(
                        GuiceRsServletContainerInitializer.class,
                        ImmutableSet.of(ServerBootstrap.class)))
            .addInitParameter(GuiceRsServerControlModule.getInitParameterKey(), GuiceRsServerControlModule.buildInitParameterValue(ServerControl.class))
            .addInitParameter(ServerBootstrap.CONFIG_INIT_PARAMETER_KEY, ce.toString())
            ;

        DeploymentManager manager = Servlets.defaultContainer()
            .addDeployment(servletBuilder);
        manager.deploy();

        PathHandler path = Handlers.path(Handlers.redirect("/"))
            .addPrefixPath("/", manager.start());

        logger.info("Starting server on {}:{}", config.getBind(), config.getPort());
        Undertow server = Undertow.builder()
            .addHttpListener(config.getPort(), config.getBind())
            .setHandler(path)
            .build();
        server.start();
    }

    private static class ServerControl
            implements GuiceRsServerControl
    {
        static Undertow server;
        static DeploymentManager manager;

        @Override
        public void stop()
        {
            server.stop();
        }

        @Override
        public void destroy()
        {
            manager.undeploy();
        }
    }
}
