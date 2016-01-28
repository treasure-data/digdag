package io.digdag.cli;

import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
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
import static io.digdag.cli.Main.systemExit;

public class Server
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    @Parameter(names = {"-t", "--port"})
    int port = 65432;

    @Parameter(names = {"-b", "--bind"})
    String bind = "127.0.0.1";

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 0) {
            throw usage(null);
        }
        server();
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag server [options...]");
        System.err.println("  Options:");
        System.err.println("    -t, --port PORT                  port number to listen for web interface and api clients (default: 65432)");
        System.err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: 127.0.0.1)");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void server()
            throws ServletException
    {
        start(port, bind, ImmutableMap.of());
    }

    public static void start(int port, String bind, Map<String, String> initParams)
            throws ServletException
    {
        DeploymentInfo servletBuilder = Servlets.deployment()
            .setClassLoader(Main.class.getClassLoader())
            .setContextPath("/digdag/server")
            .setDeploymentName("digdag-server.war")
            .addServletContainerInitalizer(
                    new ServletContainerInitializerInfo(
                        GuiceRsServletContainerInitializer.class,
                        ImmutableSet.of(ServerBootstrap.class)))
            .addInitParameter(GuiceRsServerControlModule.getInitParameterKey(), GuiceRsServerControlModule.buildInitParameterValue(ServerControl.class))
            ;
        for (Map.Entry<String, String> pair : initParams.entrySet()) {
            servletBuilder.addInitParameter(pair.getKey(), pair.getValue());
        }

        DeploymentManager manager = Servlets.defaultContainer()
            .addDeployment(servletBuilder);
        manager.deploy();

        PathHandler path = Handlers.path(Handlers.redirect("/"))
            .addPrefixPath("/", manager.start());

        logger.info("Starting server on {}:{}", bind, port);
        Undertow server = Undertow.builder()
            .addHttpListener(port, bind)
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
