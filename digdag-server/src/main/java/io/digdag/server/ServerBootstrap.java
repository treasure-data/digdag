package io.digdag.server;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.io.File;
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.server.ServerModule;
import io.digdag.server.ServerConfig;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.agent.ArchiveManager;
import io.digdag.core.agent.InProcessArchiveManager;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;

public class ServerBootstrap
    implements GuiceRsBootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    public static final String CONFIG_INIT_PARAMETER_KEY = "io.digdag.cli.server.config";

    private GuiceRsServerControl control;

    @Inject
    public ServerBootstrap(GuiceRsServerControl control)
    {
        this.control = control;
    }

    @Override
    public Injector initialize(ServletContext context)
    {
        ConfigElement systemConfig;
        String configJson = context.getInitParameter(CONFIG_INIT_PARAMETER_KEY);
        if (configJson == null) {
            systemConfig = ConfigElement.empty();
        }
        else {
            systemConfig = ConfigElement.fromJson(configJson);
        }

        ServerConfig serverConfig = ServerConfig.convertFrom(systemConfig);

        Injector injector = bootstrap(new DigdagEmbed.Bootstrap(), serverConfig)
            .initialize()
            .getInjector();

        // TODO create global site
        LocalSite site = injector.getInstance(LocalSite.class);

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

    protected DigdagEmbed.Bootstrap bootstrap(DigdagEmbed.Bootstrap bootstrap, ServerConfig serverConfig)
    {
        return bootstrap
            .setSystemConfig(serverConfig.getSystemConfig())
            .addModules(new ServerModule())
            .addModules((binder) -> {
                binder.bind(ServerConfig.class).toInstance(serverConfig);
            })
            .overrideModules((list) -> ImmutableList.of(Modules.override(list).with((binder) -> {
                binder.bind(ArchiveManager.class).to(InProcessArchiveManager.class).in(Scopes.SINGLETON);
            })));
    }

    public static void startServer(Properties props, Class<? extends ServerBootstrap> bootstrapClass)
        throws ServletException
    {
        ConfigElement ce = PropertyUtils.toConfigElement(props);
        ServerConfig config = ServerConfig.convertFrom(ce);
        startServer(config, bootstrapClass);
    }

    public static void startServer(ServerConfig config, Class<? extends ServerBootstrap> bootstrapClass)
        throws ServletException
    {
        DeploymentInfo servletBuilder = Servlets.deployment()
            .setClassLoader(bootstrapClass.getClassLoader())
            .setContextPath("/digdag/server")
            .setDeploymentName("digdag-server.war")
            .addServletContainerInitalizer(
                    new ServletContainerInitializerInfo(
                        GuiceRsServletContainerInitializer.class,
                        ImmutableSet.of(bootstrapClass)))
            .addInitParameter(GuiceRsServerControlModule.getInitParameterKey(), GuiceRsServerControlModule.buildInitParameterValue(ServerControl.class))
            .addInitParameter(CONFIG_INIT_PARAMETER_KEY, config.getSystemConfig().toString())
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
