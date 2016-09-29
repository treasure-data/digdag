package io.digdag.guice.rs.server.jmx;

import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.lang.reflect.Field;
import java.rmi.server.RemoteObject;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxAgent
{
    private static final Logger logger = LoggerFactory.getLogger(JmxAgent.class);

    private final boolean enabled;
    private final int port;

    @Inject
    public JmxAgent(JmxConfig jmxConfig)
    {
        this.enabled = jmxConfig.isEnabled();
        if (enabled) {
            this.port = jmxConfig.getPort();
        }
        else {
            this.port = 0;
        }
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
