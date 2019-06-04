package io.digdag.server;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;

import io.digdag.core.DigdagEmbed;
import io.digdag.core.ErrorReporter;
import io.digdag.client.Version;
import io.digdag.client.config.Config;
import io.digdag.core.agent.ExtractArchiveWorkspaceManager;
import io.digdag.core.agent.WorkspaceManager;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.server.metrics.DigdagMetricsModule;
import io.digdag.guice.rs.server.undertow.UndertowServer;
import io.digdag.guice.rs.server.undertow.UndertowServerControl;

import javax.servlet.ServletException;

import org.embulk.guice.Bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ServerBootstrap
    implements io.digdag.guice.rs.server.ServerBootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);

    protected final Version version;
    protected final ServerConfig serverConfig;

    public ServerBootstrap(Version version, ServerConfig serverConfig)
    {
        this.version = version;
        this.serverConfig = serverConfig;
    }

    @Override
    public Bootstrap bootstrap()
    {
        return digdagBootstrap()
                .build();
    }

    protected DigdagEmbed.Bootstrap digdagBootstrap()
    {
        return new DigdagEmbed.Bootstrap()
            .setEnvironment(serverConfig.getEnvironment())
            .setSystemConfig(serverConfig.getSystemConfig())
            //.setSystemPlugins(loadSystemPlugins(serverConfig.getSystemConfig()))
            .overrideModulesWith((binder) -> {
                binder.bind(WorkspaceManager.class).to(ExtractArchiveWorkspaceManager.class).in(Scopes.SINGLETON);
                binder.bind(Version.class).toInstance(version);
            })
            .addModules((binder) -> {
                binder.bind(ServerRuntimeInfoWriter.class).asEagerSingleton();
                binder.bind(ServerConfig.class).toInstance(serverConfig);
                binder.bind(WorkflowExecutorLoop.class).asEagerSingleton();
                binder.bind(WorkflowExecutionTimeoutEnforcer.class).asEagerSingleton();
                binder.bind(ClientVersionChecker.class).toProvider(ClientVersionCheckerProvider.class);

                binder.bind(ErrorReporter.class).to(JmxErrorReporter.class).in(Scopes.SINGLETON);
                newExporter(binder).export(ErrorReporter.class).withGeneratedName();
            })
            .addModules(new ServerModule(serverConfig))
            .addModules(new DigdagMetricsModule())
        ;
    }

    public static GuiceRsServerControl start(ServerBootstrap bootstrap)
        throws ServletException
    {
        UndertowServerControl control = UndertowServer.start(bootstrap.serverConfig, bootstrap);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            control.stop();
            control.destroy();
            logger.info("Shutdown completed");
        }, "shutdown"));

        return control;
    }

    private static class ClientVersionCheckerProvider
            implements Provider<ClientVersionChecker>
    {
        private final Config systemConfig;

        @Inject
        public ClientVersionCheckerProvider(Config systemConfig)
        {
            this.systemConfig = systemConfig;
        }

        @Override
        public ClientVersionChecker get()
        {
            return ClientVersionChecker.fromSystemConfig(systemConfig);
        }
    }
}
