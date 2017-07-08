package io.digdag.guice.rs.server.undertow;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServerRuntimeInfo;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.guice.rs.server.ServerBootstrap;
import io.digdag.guice.rs.server.undertow.UndertowServerConfig.ListenAddress;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.conduits.GzipStreamSourceConduit;
import io.undertow.conduits.InflatingStreamSourceConduit;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.OpenListener;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import io.undertow.server.handlers.accesslog.DefaultAccessLogReceiver;
import io.undertow.server.handlers.encoding.ContentEncodingRepository;
import io.undertow.server.handlers.encoding.DeflateEncodingProvider;
import io.undertow.server.handlers.encoding.EncodingHandler;
import io.undertow.server.handlers.encoding.GzipEncodingProvider;
import io.undertow.server.handlers.encoding.RequestEncodingHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ServletContainerInitializerInfo;
import io.undertow.servlet.spec.HttpServletRequestImpl;
import io.undertow.util.AttachmentKey;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import static java.util.Locale.ENGLISH;

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

    private static AttachmentKey<String> LISTEN_ADDRESS_NAME_ATTACHMENT = AttachmentKey.create(String.class);

    static class SetListenAddressNameHandler
            implements HttpHandler
    {
        private final HttpHandler next;
        private final String name;

        public SetListenAddressNameHandler(HttpHandler next, String name)
        {
            this.next = next;
            this.name = name;
        }

        @Override
        public void handleRequest(HttpServerExchange exchange)
                throws Exception
        {
            exchange.putAttachment(LISTEN_ADDRESS_NAME_ATTACHMENT, name);
            next.handleRequest(exchange);
        }
    }

    static class SetListenAddressNameServletFilter
            implements Filter
    {
        @Override
        public void init(FilterConfig filterConfig)
        { }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException
        {
            if (request instanceof HttpServletRequestImpl) {
                HttpServerExchange exchange = ((HttpServletRequestImpl) request).getExchange();
                String name = exchange.getAttachment(LISTEN_ADDRESS_NAME_ATTACHMENT);
                if (name != null) {
                    request.setAttribute(GuiceRsServerRuntimeInfo.LISTEN_ADDRESS_NAME_ATTRIBUTE, name);
                }
                chain.doFilter(request, response);
            }
        }

        @Override
        public void destroy()
        { }
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
            .addFilter(new FilterInfo("AddListenAddressName", SetListenAddressNameServletFilter.class))
            .addFilterUrlMapping("AddListenAddressName", "/*", DispatcherType.REQUEST)
            .addServletContainerInitalizer(
                    new ServletContainerInitializerInfo(
                        GuiceRsServletContainerInitializer.class,
                        ImmutableSet.of(UndertowBootstrap.class)))
            ;

        DeploymentManager deployment = Servlets.defaultContainer()
            .addDeployment(servletBuilder);

        control.deploymentInitialized(deployment);
        deployment.deploy();  // GuiceRsBootstrap.initialize runs here

        final HttpHandler appHandler;
        {
            HttpHandler handler = Handlers.path(Handlers.redirect("/"))
                .addPrefixPath("/", deployment.start());
            if (config.getAccessLogPath().isPresent()) {
                handler = buildAccessLogHandler(config, handler);
            }
            handler = Handlers.trace(handler);

            // support "Content-Encoding: gzip | deflate" (request content encoding)
            handler = new RequestEncodingHandler(handler)
                .addEncoding("deflate", InflatingStreamSourceConduit.WRAPPER)
                .addEncoding("gzip", GzipStreamSourceConduit.WRAPPER);
            // support "Accept-Encoding: gzip | deflate" (response content encoding)
            handler = new EncodingHandler(handler,
                    new ContentEncodingRepository()
                    .addEncodingHandler("deflate", new DeflateEncodingProvider(), 50)
                    .addEncodingHandler("gzip", new GzipEncodingProvider(), 60));

            appHandler = handler;
        }

        // wrap HttpHandler in GracefulShutdownHandler
        Map<GracefulShutdownHandler, ListenAddress> httpHandlers = new HashMap<>();
        for (ListenAddress addr : config.getListenAddresses()) {
            GracefulShutdownHandler httpHandler = Handlers.gracefulShutdown(appHandler);
            httpHandlers.put(httpHandler, addr);
            control.addHandler(httpHandler);
        }

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

        logger.info("Starting server on {}",
                config.getListenAddresses().stream()
                .map(a -> a.getBind() + ":" + a.getPort())
                .collect(Collectors.joining(", ")));
        Undertow.Builder serverBuilder = Undertow.builder()
            .setWorker(worker)
            .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)  // required to enable reqtime:%T in access log
            .setServerOption(UndertowOptions.NO_REQUEST_TIMEOUT, config.getHttpNoRequestTimeout().or(60) * 1000)
            .setServerOption(UndertowOptions.REQUEST_PARSE_TIMEOUT, config.getHttpRequestParseTimeout().or(30) * 1000)
            .setServerOption(UndertowOptions.IDLE_TIMEOUT, config.getHttpIoIdleTimeout().or(300) * 1000)
            .setServerOption(UndertowOptions.ENABLE_HTTP2, config.getEnableHttp2())
            ;

        for (Map.Entry<GracefulShutdownHandler, ListenAddress> entry : httpHandlers.entrySet()) {
            ListenAddress addr = entry.getValue();
            if (addr.getSslContext().isPresent()) {
                // HTTPS
                serverBuilder.addHttpsListener(addr.getPort(), addr.getBind(), addr.getSslContext().get(), entry.getKey());
            }
            else {
                // HTTP
                serverBuilder.addHttpListener(addr.getPort(), addr.getBind(), entry.getKey());
            }
        }

        Undertow server = serverBuilder.build();
        control.serverInitialized(server);

        server.start();  // HTTP server starts here

        // collect local addresses
        for (Undertow.ListenerInfo listenerInfo : server.getListenerInfo()) {
            OpenListener listener = httpListenerOf(listenerInfo);
            SocketAddress listenerAddress = listenerInfo.getAddress();
            HttpHandler handler = listener.getRootHandler();
            ListenAddress listenAddressConfig = httpHandlers.get(handler);
            if (listenAddressConfig != null && listenerAddress instanceof InetSocketAddress) {
                InetSocketAddress inet = (InetSocketAddress) listenerAddress;
                logger.info("Bound on {}:{} ({})", inet.getHostString(), inet.getPort(), listenAddressConfig.getName());
                control.getRuntimeInfo().addListenAddress(listenAddressConfig.getName(), inet);
                listener.setRootHandler(new SetListenAddressNameHandler(handler, listenAddressConfig.getName()));
            }
            else {
                logger.warn("Unknown listener {} bound on {}", listenerInfo, listenerAddress);
            }
        }

        control.serverStarted();

        control.postStart();

        return control;
    }

    private static OpenListener httpListenerOf(Undertow.ListenerInfo listenerInfo)
    {
        try {
            Field listenerField = Undertow.ListenerInfo.class.getDeclaredField("openListener");
            listenerField.setAccessible(true);
            return (OpenListener) listenerField.get(listenerInfo);
        }
        catch (NoSuchFieldException | IllegalAccessException | ClassCastException e) {
            throw new AssertionError("Failed to get local listen address", e);
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
