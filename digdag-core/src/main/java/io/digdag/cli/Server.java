package io.digdag.cli;

import java.util.List;
import javax.servlet.ServletException;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.undertow.Undertow;
import io.undertow.Handlers;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import io.digdag.cli.Main.SystemExitException;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.server.ServerBootstrap;
import io.digdag.core.LocalSite;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Server
{
    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 0) {
            throw usage(null);
        }

        new Server().server();
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag server [options...]");
        System.err.println("  Options:");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    public void server()
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
        DeploymentManager manager = Servlets.defaultContainer()
            .addDeployment(servletBuilder);
        manager.deploy();

        PathHandler path = Handlers.path(Handlers.redirect("/"))
            .addPrefixPath("/", manager.start());

        Undertow server = Undertow.builder()
            .addHttpListener(9090, "localhost")
            .setHandler(path)
            .build();
        server.start();
    }
}
