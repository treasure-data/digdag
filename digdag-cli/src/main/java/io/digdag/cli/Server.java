package io.digdag.cli;

import java.util.List;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableSet;
import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.server.ServerBootstrap;
import static io.digdag.cli.Main.systemExit;

public class Server
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

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
        System.err.println("Usage: digdag server [options...] [workflow.yml]");
        System.err.println("  Options:");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    private void server(String workflowPath)
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
            ;

        if (workflowPath != null) {
            servletBuilder.addInitParameter("io.digdag.server.workflowPath", workflowPath);
        }

        DeploymentManager manager = Servlets.defaultContainer()
            .addDeployment(servletBuilder);
        manager.deploy();

        PathHandler path = Handlers.path(Handlers.redirect("/"))
            .addPrefixPath("/", manager.start());

        logger.info("Starting server on 127.0.0.1:9090");
        Undertow server = Undertow.builder()
            .addHttpListener(9090, "127.0.0.1")
            .setHandler(path)
            .build();
        server.start();
    }
}
