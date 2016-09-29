package io.digdag.guice.rs.server.undertow;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Injector;

import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsBootstrap;
import io.digdag.guice.rs.GuiceRsServerControl;
import io.digdag.guice.rs.GuiceRsServletContainerInitializer;
import io.digdag.guice.rs.server.ServerBootstrap;
import io.digdag.guice.rs.server.ServerLifeCycleModule;
import io.digdag.guice.rs.server.jmx.JmxConfig;
import io.digdag.guice.rs.server.jmx.JmxModule;

import org.embulk.guice.Bootstrap;
import org.embulk.guice.CloseableInjector;

import java.net.InetSocketAddress;
import java.util.List;
import javax.servlet.ServletContext;

public class UndertowBootstrap
    implements GuiceRsBootstrap
{
    private final UndertowServerControl control;
    private final UndertowServerConfig config;
    private final ServerBootstrap serverBootstrap;

    @Inject
    public UndertowBootstrap()
    {
        UndertowServer.InitializeContext initContext = UndertowServer.getInitializeContext();
        this.control = initContext.getControl();
        this.config = initContext.getConfig();
        this.serverBootstrap = initContext.getServerBootstrap();
    }

    @Override
    public Injector initialize(ServletContext context)
    {
        Bootstrap bootstrap = serverBootstrap.bootstrap();

        bootstrap.addModules(new ServerLifeCycleModule());

        bootstrap.addModules((binder) -> {
            binder.bind(GuiceRsServerControl.class).toInstance(control);

            binder.bind(UndertowServerInfo.class).toInstance(new UndertowServerInfo() {
                @Override
                public boolean isStarted()
                {
                    return control.isServerStarted();
                }

                @Override
                public List<InetSocketAddress> getLocalAddresses()
                {
                    return control.getLocalAddresses();
                }
            });
        });

        bootstrap.addModules(new JmxModule(), (binder) -> {
            final Optional<Integer> jmxPort = config.getJmxPort();

            binder.bind(JmxConfig.class).toInstance(new JmxConfig()
            {
                @Override
                public boolean isEnabled()
                {
                    return jmxPort.isPresent();
                }

                @Override
                public int getPort()
                {
                    return jmxPort.or(0);
                }
            });
        });

        // GuiceRsServletContainerInitializer closes CloseableInjector (Injector implementing AutoCloseable)
        // when servlet context is destroyed.
        CloseableInjector injector = bootstrap.initializeCloseable();
        control.injectorInitialized(injector);
        return injector;
    }
}

