package io.digdag.guice.rs.server.undertow;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.guice.rs.server.ServerBootstrap;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.OpenListener;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import io.undertow.server.handlers.accesslog.DefaultAccessLogReceiver;
import io.undertow.server.protocol.http.HttpOpenListener;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;

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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class UndertowServer
{
    private static final Logger logger = LoggerFactory.getLogger(UndertowServer.class);

    private UndertowServer()
    { }

    static class InitializeContext
    {
        private final UndertowServerControl control;
        private final UndertowServerConfig config;
        private final ServerBootstrap serverBootstrap;

        public InitializeContext(
            UndertowServerControl control,
            UndertowServerConfig config,
            ServerBootstrap serverBootstrap)
        {
            this.control = control;
            this.config = config;
            this.serverBootstrap = serverBootstrap;
        }

        UndertowServerControl getControl()
        {
            return control;
        }

        UndertowServerConfig getConfig()
        {
            return config;
        }

        ServerBootstrap getServerBootstrap()
        {
            return serverBootstrap;
        }
    }

    private static final InheritableThreadLocal<InitializeContext> initializeContext
        = new InheritableThreadLocal<>();

    static InitializeContext getInitializeContext()
    {
        return initializeContext.get();
    }

    public static UndertowServerControl start(UndertowServerConfig config, ServerBootstrap bootstrap)
        throws ServletException
    {
        UndertowServerControl control = new UndertowServerControl();

        initializeContext.set(new InitializeContext(control, config, bootstrap));

        DeploymentInfo servletBuilder = Servlets.deployment()
            .setClassLoader(bootstrap.getClass().getClassLoader())
            .setContextPath("/rs")
            .setDeploymentName("rs.war")
            .addServletContainerInitalizer(
                    new ServletContainerInitializerInfo(
                        GuiceRsServletContainerInitializer.class,
                        ImmutableSet.of(UndertowBootstrap.class)))
            ;

        DeploymentManager deployment = Servlets.defaultContainer()
            .addDeployment(servletBuilder);

        control.deploymentInitialized(deployment);
        deployment.deploy();  // GuiceRsBootstrap.initialize runs here

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
            int httpIoThreads = config.getHttpIoThreads().or(() -> Math.max(Runtime.getRuntime().availableProcessors(), 2));
            int httpWorkerThreads = config.getHttpWorkerThreads().or(httpIoThreads * 8);

            // WORKER_TASK_CORE_THREADS is not used since this commit:
            // https://github.com/xnio/xnio/commit/fc16271f732630d9f1486d9d23eebb01a5159bb9
            // Xnio always uses corePoolSize == maximumPoolSize which means that all threads are
            // always alive even if they're idle.

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

        HttpHandler apiHandler = exchange -> httpHandler.handleRequest(exchange);
        HttpHandler adminHandler = exchange -> httpHandler.handleRequest(exchange);

        logger.info("Starting server on {}:{}", config.getBind(), config.getPort());
        Undertow server = Undertow.builder()
            .addHttpListener(config.getPort(), config.getBind(), apiHandler)
            .addHttpListener(config.getAdminPort(), config.getAdminBind(), adminHandler)
            .setWorker(worker)
            .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)  // required to enable reqtime:%T in access log
            .setServerOption(UndertowOptions.NO_REQUEST_TIMEOUT, config.getHttpNoRequestTimeout().or(60) * 1000)
            .setServerOption(UndertowOptions.REQUEST_PARSE_TIMEOUT, config.getHttpRequestParseTimeout().or(30) * 1000)
            .setServerOption(UndertowOptions.IDLE_TIMEOUT, config.getHttpIoIdleTimeout().or(300) * 1000)
            .build();
        control.serverInitialized(server);

        server.start();  // HTTP server starts here

        List<InetSocketAddress> localAddresses = new ArrayList<>();
        List<InetSocketAddress> localAdminAddresses = new ArrayList<>();

        for (Undertow.ListenerInfo listenerInfo : server.getListenerInfo()) {
            OpenListener listener = listener(listenerInfo);
            if (listener instanceof HttpOpenListener) {
                HttpHandler rootHandler = listener.getRootHandler();
                if (!(listenerInfo.getAddress() instanceof InetSocketAddress)) {
                    continue;
                }
                InetSocketAddress address = (InetSocketAddress) listenerInfo.getAddress();
                if (rootHandler == apiHandler) {
                    logger.info("Bound api on {}", address);
                    localAddresses.add(address);
                }
                else if (rootHandler == adminHandler) {
                    logger.info("Bound admin api on {}", address);
                    localAdminAddresses.add(address);
                } else {
                    logger.warn("Unknown listener {} bound on {}", listenerInfo, address);
                }
            }
        }

        control.serverStarted(localAddresses, localAdminAddresses);

        control.postStart();

        return control;
    }

    private static OpenListener listener(Undertow.ListenerInfo listenerInfo)
    {
        try {
            Field listenerField = Undertow.ListenerInfo.class.getDeclaredField("openListener");
            listenerField.setAccessible(true);
            return (OpenListener) listenerField.get(listenerInfo);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            logger.warn("Failed to get local address", e);
            return null;
        }
    }

    private static HttpHandler buildAccessLogHandler(UndertowServerConfig config, HttpHandler nextHandler)
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
                        GuiceRsBootstrap.class.getClassLoader()));
        }
        else {
            return new AccessLogHandler(nextHandler, logReceiver,
                    config.getAccessLogPattern(),
                    GuiceRsBootstrap.class.getClassLoader());
        }
    }
}
