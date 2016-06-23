package io.digdag.server;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import io.undertow.server.handlers.accesslog.DefaultAccessLogReceiver;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.guice.rs.GuiceRsServerControlModule;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.config.PropertyUtils;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.core.agent.LocalWorkspaceManager;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;

public class ServerBootstrap
    implements GuiceRsBootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    public static final String CONFIG_INIT_PARAMETER_KEY = "io.digdag.cli.server.config";
    public static final String VERSION_INIT_PARAMETER_KEY = "io.digdag.cli.server.version";

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

        Version version = Version.of(context.getInitParameter(VERSION_INIT_PARAMETER_KEY));

        Injector injector = bootstrap(new DigdagEmbed.Bootstrap(), serverConfig, version)
            .initialize()
            .getInjector();

        if (serverConfig.getExecutorEnabled()) {
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
        }

        return injector;
    }

    protected DigdagEmbed.Bootstrap bootstrap(DigdagEmbed.Bootstrap bootstrap, ServerConfig serverConfig, Version version)
    {
        return bootstrap
            .setSystemConfig(serverConfig.getSystemConfig())
            //.setSystemPlugins(loadSystemPlugins(serverConfig.getSystemConfig()))
            .overrideModulesWith((binder) -> {
                binder.bind(WorkspaceManager.class).to(LocalWorkspaceManager.class).in(Scopes.SINGLETON);
                binder.bind(Version.class).toInstance(version);
            })
            .addModules((binder) -> {
                binder.bind(ServerConfig.class).toInstance(serverConfig);
            })
            .addModules(new ServerModule());
    }

    public static void startServer(Version version, Properties props, Class<? extends ServerBootstrap> bootstrapClass)
        throws ServletException
    {
        ConfigElement ce = PropertyUtils.toConfigElement(props);
        ServerConfig config = ServerConfig.convertFrom(ce);
        startServer(version, config, bootstrapClass);
    }

    public static void startServer(Version version, ServerConfig config, Class<? extends ServerBootstrap> bootstrapClass)
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
            .addInitParameter(VERSION_INIT_PARAMETER_KEY, version.toString())
            ;

        DeploymentManager manager = Servlets.defaultContainer()
            .addDeployment(servletBuilder);
        manager.deploy();

        PathHandler path = Handlers.path(Handlers.redirect("/"))
            .addPrefixPath("/", manager.start());

        HttpHandler handler;
        if (config.getAccessLogPath().isPresent()) {
            handler = buildAccessLogHandler(config, path);
        }
        else {
            handler = path;
        }

        logger.info("Starting server on {}:{}", config.getBind(), config.getPort());
        Undertow server = Undertow.builder()
            .addHttpListener(config.getPort(), config.getBind())
            .setHandler(handler)
            .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)  // required to enable reqtime:%T in access log
            .build();
        server.start();
    }

    private static HttpHandler buildAccessLogHandler(ServerConfig config, HttpHandler nextHandler)
    {
        Path path = Paths.get(config.getAccessLogPath().get()).toAbsolutePath().normalize();

        try {
            Files.createDirectories(path);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        Executor logWriterExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(false)  // non-daemon
                .setNameFormat("access-log-%d")
                .build()
                );

        AccessLogReceiver logReceiver = new DefaultAccessLogReceiver(logWriterExecutor, path.toFile(), "access.", "log");

        if (JsonLogFormatter.isJsonPattern(config.getAccessLogPattern())) {
            return new AccessLogHandler(nextHandler, logReceiver,
                    config.getAccessLogPattern(),  // this name is used by AccessLogHandler.toString
                    JsonLogFormatter.buildExchangeAttribute(
                        config.getAccessLogPattern(),
                        ServerBootstrap.class.getClassLoader()));
        }
        else {
            return new AccessLogHandler(nextHandler, logReceiver,
                    config.getAccessLogPattern(),
                    ServerBootstrap.class.getClassLoader());
        }
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
