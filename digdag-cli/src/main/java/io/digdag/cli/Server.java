package io.digdag.cli;

import java.util.List;
import java.util.TimeZone;
import java.io.File;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.guice.rs.GuiceRsServerControlModule;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.server.ServerModule;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;

public class Server
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    @Parameter(names = {"-p", "--port"})
    int port = 9090;

    @Parameter(names = {"-b", "--bind"})
    String bind = "127.0.0.1";

    @Parameter(names = {"-a", "--auto-load"})
    String autoLoadFile = null;

    @Override
    public void main()
            throws Exception
    {
        String workflowPath;
        switch (args.size()) {
        case 0:
            workflowPath = null;
            break;
        case 1:
            workflowPath = args.get(0);
            break;
        default:
            throw usage(null);
        }
        server(workflowPath);
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag server [options...]");
        System.err.println("  Options:");
        System.err.println("    -p, --port PORT                  port number to listen HTTP clients (default: 9090)");
        System.err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: 127.0.0.1)");
        System.err.println("    -a, --auto-load PATH.yml         load this file to register schedules and reload it automatically");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void server(String workflowPath)
            throws ServletException
    {
        ServerBootstrap.cmd = this;

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

    public static class ServerBootstrap
        implements GuiceRsBootstrap
    {
        private static Server cmd;

        private GuiceRsServerControl control;

        @Inject
        public ServerBootstrap(GuiceRsServerControl control)
        {
            this.control = control;
        }

        @Override
        public Injector initialize(ServletContext context)
        {
            Injector injector = new DigdagEmbed.Bootstrap()
                .addModules(new ServerModule())
                .addModules((binder) -> {
                    binder.bind(ArgumentConfigLoader.class).in(Scopes.SINGLETON);
                    binder.bind(RevisionAutoReloader.class).in(Scopes.SINGLETON);
                })
                .initialize()
                .getInjector();

            // TODO create global site
            LocalSite site = injector.getInstance(LocalSite.class);

            if (cmd.autoLoadFile != null) {
                ConfigFactory cf = injector.getInstance(ConfigFactory.class);
                RevisionAutoReloader autoReloader = injector.getInstance(RevisionAutoReloader.class);
                try {
                    autoReloader.loadFile(new File(cmd.autoLoadFile), TimeZone.getDefault(), cf.create());
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            // start server
            site.startLocalAgent();
            site.startMonitor();

            Thread thread = new Thread(() -> {
                try {
                    site.run();
                }
                catch (Exception ex) {
                    logger.error("Uncaught error", ex);
                    control.destroy();
                }
            }, "local-site");
            thread.setDaemon(true);
            thread.start();

            return injector;
        }
    }
}
