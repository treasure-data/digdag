package io.digdag.server;

import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.google.inject.matcher.AbstractMatcher;

import io.digdag.client.config.ConfigElement;
import io.digdag.core.ErrorReporter;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.rmi.server.RemoteObject;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MBeanServer;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.guice.MBeanModule;

import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class JmxModule
    implements Module
{
    private static final Logger logger = LoggerFactory.getLogger(JmxModule.class);

    @Override
    public void configure(Binder binder)
    {
        new MBeanModule().configure(binder);
        binder.bind(MBeanServer.class).toInstance(ManagementFactory.getPlatformMBeanServer());
        binder.bind(JmxAgent.class).asEagerSingleton();
        binder.bind(ErrorReporter.class).to(JmxErrorReporter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ErrorReporter.class).withGeneratedName();
    }

    private static class JmxAgent
    {
        private final boolean enabled;
        private final int port;

        @Inject
        public JmxAgent(ServerConfig config)
        {
            this.enabled = config.getJmxPort().isPresent();
            this.port = config.getJmxPort().or(0);
        }

        @PostConstruct
        public void start()
        {
            if (enabled) {
                startAgent(port);
            }
        }

        private static void startAgent(int port)
        {
            System.setProperty("com.sun.management.jmxremote", "true");
            System.setProperty("com.sun.management.jmxremote.port", Integer.toString(port));
            System.setProperty("com.sun.management.jmxremote.rmi.port", Integer.toString(port));
            System.setProperty("com.sun.management.jmxremote.authenticate", "false");
            System.setProperty("com.sun.management.jmxremote.ssl", "false");

            try {
                sun.management.Agent.startAgent();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }

            logger.info("JMX agent started on port {}", getJmxRegistryPort(port));
        }

        private static int getJmxRegistryPort(int defaultPort)
        {
            try {
                Object jmxServer = getField(sun.management.Agent.class, "jmxServer");
                if (jmxServer == null) {
                    logger.warn("Failed to determine JMX port: sun.management.Agent.jmxServer field is null");
                }
                else {
                    Object registry = getField(sun.management.jmxremote.ConnectorBootstrap.class, "registry");
                    if (registry == null) {
                        logger.warn("Failed to determine JMX port: sun.management.jmxremote.ConnectorBootstrap.registry field is null");
                    }
                    else {
                        return sun.rmi.server.UnicastRef.class.cast(RemoteObject.class.cast(registry).getRef()).getLiveRef().getPort();
                    }
                }
                return defaultPort;
            }
            catch (Exception e) {
                logger.warn("Failed to determine JMX port", e);
                return defaultPort;
            }
        }

        private static Object getField(Class<?> clazz, String name)
                throws ReflectiveOperationException
        {
            Field field = clazz.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(clazz);
        }

        // Agent class doesn't have methods to stop agent
    }
}
