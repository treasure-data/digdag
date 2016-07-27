package io.digdag.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.Version;
import io.digdag.core.agent.LocalWorkspaceManager;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.core.config.PropertyUtils;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.GuiceRsServerControlModule;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import io.undertow.server.handlers.accesslog.DefaultAccessLogReceiver;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ServerBootstrap
    implements GuiceRsBootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    public static final String CONFIG_INIT_PARAMETER_KEY = "io.digdag.cli.server.config";
    public static final String VERSION_INIT_PARAMETER_KEY = "io.digdag.cli.server.version";

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

        // GuiceRsServletContainerInitializer closes Digdag (through Injector implementing AutoCloseable)
        // when servlet context is destroyed.
        DigdagEmbed digdag = bootstrap(new DigdagEmbed.Bootstrap(), serverConfig, version)
            .initializeWithoutShutdownHook();

        Injector injector = digdag.getInjector();

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

    private static final InheritableThreadLocal<GuiceRsServerControl> servletServerControl = new InheritableThreadLocal<>();

    private static class ServerControl
            implements GuiceRsServerControl
    {
        private final DeploymentManager deployment;
        private Undertow server;
        private XnioWorker worker;
        private List<GracefulShutdownHandler> handlers;

        public ServerControl(DeploymentManager deployment)
        {
            this.deployment = deployment;
            this.server = null;
            this.worker = null;
            this.handlers = new ArrayList<>();
        }

        void workerInitialized(XnioWorker worker)
        {
            this.worker = worker;
        }

        void addHandler(GracefulShutdownHandler handler)
        {
            handlers.add(handler);
        }

        void serverInitialized(Undertow server)
        {
            this.server = server;
        }

        @Override
        public void stop()
        {
            // closes HTTP listening channels
            logger.info("Closing HTTP listening sockets");
            if (server != null) {
                server.stop();
            }

            // HTTP handlers will return 503 Service Unavailable for new requests
            for (GracefulShutdownHandler handler : handlers) {
                handler.shutdown();
            }

            // waits for completion of currently handling requests upto 30 seconds
            logger.info("Waiting for completion of running HTTP requests...");
            for (GracefulShutdownHandler handler : handlers) {
                try {
                    handler.awaitShutdown(30 * 1000);
                }
                catch (InterruptedException ex) {
                    logger.info("Interrupted. Force shutting down running requests");
                }
            }

            // kills all processing threads. These threads should be already
            // idling because there're no currently handling requests.
            logger.info("Shutting down HTTP worker threads");
            if (worker != null) {
                worker.shutdownNow();
            }
        }

        @Override
        public void destroy()
        {
            logger.info("Shutting down system");
            try {
                // calls Servlet.destroy that is GuiceRsApplicationServlet.destroy defined at
                // RESTEasy HttpServlet30Dispatcher class
                deployment.stop();
            }
            catch (ServletException ex) {
                throw Throwables.propagate(ex);
            }
            finally {
                // calls ServletContextListener.contextDestroyed that calls @PreDestroy hooks
                // through GuiceRsServletContainerInitializer.CloseableInjectorDestroyListener listener
                deployment.undeploy();
            }
        }
    }

    private static class ThreadLocalServerControlProvider
            implements Provider<GuiceRsServerControl>
    {
        @Override
        public GuiceRsServerControl get()
        {
            return servletServerControl.get();
        }
    }

    private GuiceRsServerControl control;

    public static void startServer(Version version, Properties props, Class<? extends ServerBootstrap> bootstrapClass)
        throws ServletException
    {
        ConfigElement ce = PropertyUtils.toConfigElement(props);
        ServerConfig config = ServerConfig.convertFrom(ce);

        GuiceRsServerControl control = startServer(version, config, bootstrapClass);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            control.stop();
            control.destroy();
        }, "shutdown"));
    }

    public static GuiceRsServerControl startServer(Version version, ServerConfig config, Class<? extends ServerBootstrap> bootstrapClass)
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
            .addInitParameter(GuiceRsServerControlModule.getInitParameterKey(), GuiceRsServerControlModule.buildInitParameterValue(ThreadLocalServerControlProvider.class))
            .addInitParameter(CONFIG_INIT_PARAMETER_KEY, config.getSystemConfig().toString())
            .addInitParameter(VERSION_INIT_PARAMETER_KEY, version.toString())
            ;

        DeploymentManager deployment = Servlets.defaultContainer()
            .addDeployment(servletBuilder);

        ServerControl control = new ServerControl(deployment);
        servletServerControl.set(control);

        deployment.deploy();  // ServerBootstrap.initialize runs here

        GracefulShutdownHandler httpHandler;
        {
            HttpHandler handler = Handlers.path(Handlers.redirect("/"))
                .addPrefixPath("/", deployment.start());

            if (config.getAccessLogPath().isPresent()) {
                handler = buildAccessLogHandler(config, handler);
            }

            httpHandler = Handlers.gracefulShutdown(handler);
        }
        control.addHandler(httpHandler);

        XnioWorker worker;
        try {
            // copied defaults from Undertow
            int httpIoThreads = Math.max(Runtime.getRuntime().availableProcessors(), 2);
            int httpWorkerThreads = httpIoThreads;

            worker = Xnio.getInstance(Undertow.class.getClassLoader())
                .createWorker(OptionMap.builder()
                        .set(Options.WORKER_IO_THREADS, httpIoThreads)
                        .set(Options.CONNECTION_HIGH_WATER, 1000000)
                        .set(Options.CONNECTION_LOW_WATER, 1000000)
                        .set(Options.WORKER_TASK_CORE_THREADS, httpWorkerThreads)
                        .set(Options.WORKER_TASK_MAX_THREADS, httpWorkerThreads)
                        .set(Options.TCP_NODELAY, true)
                        .set(Options.CORK, true)
                        .getMap());
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
        control.workerInitialized(worker);

        logger.info("Starting server on {}:{}", config.getBind(), config.getPort());
        Undertow server = Undertow.builder()
            .addHttpListener(config.getPort(), config.getBind())
            .setHandler(httpHandler)
            .setWorker(worker)
            .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)  // required to enable reqtime:%T in access log
            .build();
        control.serverInitialized(server);

        server.start();  // HTTP server starts here

        // XXX (dano): Hack to get the system-assigned port when starting server with port 0
        List<InetSocketAddress> localAddresses = new ArrayList<>();
        try {
            Field channelsField = Undertow.class.getDeclaredField("channels");
            channelsField.setAccessible(true);
            @SuppressWarnings("unchecked") List<AcceptingChannel<? extends StreamConnection>> channels = (List<AcceptingChannel<? extends StreamConnection>>) channelsField.get(server);
            for (AcceptingChannel<? extends StreamConnection> channel : channels) {
                SocketAddress localAddress = channel.getLocalAddress();
                logger.info("Bound on {}", localAddress);
                if (localAddress instanceof InetSocketAddress) {
                    localAddresses.add((InetSocketAddress) localAddress);
                }
            }
        }
        catch (ReflectiveOperationException e) {
            logger.warn("Failed to get bind addresses", e);
        }

        Optional<String> serverInfoPath = config.getServerRuntimeInfoPath();
        if (serverInfoPath.isPresent()) {
            ServerRuntimeInfo serverRuntimeInfo = ServerRuntimeInfo.builder()
                    .addAllLocalAddresses(localAddresses.stream()
                            .map(a -> ServerRuntimeInfo.Address.of(a.getHostString(), a.getPort()))
                            .collect(Collectors.toList()))
                    .build();
            ObjectMapper mapper = new ObjectMapper();
            try {
                Files.write(Paths.get(serverInfoPath.get()), mapper.writeValueAsBytes(serverRuntimeInfo));
            }
            catch (IOException e) {
                logger.warn("Failed to write server runtime info", e);
            }
        }

        return control;
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
}
