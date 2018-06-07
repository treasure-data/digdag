package io.digdag.guice.rs.server.jmx;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.rmi.server.RemoteObject;
import java.util.Properties;
import java.util.stream.IntStream;
import javax.annotation.PostConstruct;
import javax.management.remote.JMXServiceURL;

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
        try {
            Class.forName("sun.management.Agent");
            // If sun.management.Agent exists, it's JDK 8 or older. Use legacy reflection-based code
            // which doesn't work since JDK 9.
            try {
                startAgent8(port);
                return;
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        catch (ReflectiveOperationException e) {
            // expected since JDK 9
        }

        Properties props = new Properties();
        props.setProperty("com.sun.management.jmxremote.port", Integer.toString(port));
        props.setProperty("com.sun.management.jmxremote.rmi.port", Integer.toString(port));
        props.setProperty("com.sun.management.jmxremote.authenticate", "false");
        props.setProperty("com.sun.management.jmxremote.ssl", "false");

        try {
            long pid = getPid();
            // Here attaches to this VM. This is prohibited by default since JDK 9 due to
            // sun.tools.attach.HotSpotVirtualMachine.ALLOW_ATTACH_SELF flag. To bypass this
            // problem, jdk.attach.allowAttachSelf=true system property is necessary.
            VirtualMachine virtualMachine = VirtualMachine.attach(Long.toString(pid));
            try {
                virtualMachine.startLocalManagementAgent();
                virtualMachine.startManagementAgent(props);
            }
            finally {
                virtualMachine.detach();
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to start JMX agent. Please make sure that you add '-Djdk.attach.allowAttachSelf=true' to the command line options of java or to JDK_JAVA_OPTIONS environment variable.", e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            JMXServiceURL url = new JMXServiceURL("rmi", null, port);
            int actualPort = url.getPort();
            logger.info("JMX agent started on port {}", actualPort);
        }
        catch (Exception e) {
            logger.warn("Failed to determine JMX port", e);
        }
    }

    private static long getPid()
    {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int index = name.indexOf('@');
        return Long.parseLong(name.substring(0, index));
    }

    private static void startAgent8(int port)
            throws ReflectiveOperationException
    {
        System.setProperty("com.sun.management.jmxremote", "true");
        System.setProperty("com.sun.management.jmxremote.port", Integer.toString(port));
        System.setProperty("com.sun.management.jmxremote.rmi.port", Integer.toString(port));
        System.setProperty("com.sun.management.jmxremote.authenticate", "false");
        System.setProperty("com.sun.management.jmxremote.ssl", "false");
        invokeStaticMethod("sun.management.Agent", "startAgent");
        try {
            int actualPort = getJmxRegistryPort8(port);
            logger.info("JMX agent started on port {}", actualPort);
        }
        catch (Exception e) {
            logger.warn("Failed to determine JMX port", e);
        }
    }

    private static int getJmxRegistryPort8(int defaultPort)
            throws ReflectiveOperationException
    {
        Object jmxServer = getDeclaredStaticField("sun.management.Agent", "jmxServer");
        if (jmxServer == null) {
            logger.warn("Failed to determine JMX port: sun.management.Agent.jmxServer field is null");
        }
        else {
            Object registry = getDeclaredStaticField("sun.management.jmxremote.ConnectorBootstrap", "registry");
            if (registry == null) {
                logger.warn("Failed to determine JMX port: sun.management.jmxremote.ConnectorBootstrap.registry field is null");
            }
            else {
                return (int) invokeMethod(invokeMethod(RemoteObject.class.cast(registry).getRef(), "getLiveRef"), "getPort");
            }
        }
        return defaultPort;
    }

    private static Object getDeclaredStaticField(String className, String fieldName)
            throws ReflectiveOperationException
    {
        Class clazz = Class.forName(className);
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(clazz);
    }

    private static Object invokeStaticMethod(String className, String methodName, Object... typesArgs)
            throws ReflectiveOperationException
    {
        Class[] types = IntStream.range(0, typesArgs.length).filter(i -> i % 2 == 0)
            .mapToObj(i -> (Class) typesArgs[i]).toArray(Class[]::new);
        Object[] args = IntStream.range(0, typesArgs.length).filter(i -> i % 2 == 1)
            .mapToObj(i -> typesArgs[i]).toArray(Object[]::new);
        Method method = Class.forName(className).getMethod(methodName, types);
        return method.invoke(null, args);
    }

    private static Object invokeMethod(Object object, String methodName, Object... typesArgs)
            throws ReflectiveOperationException
    {
        Class[] types = IntStream.range(0, typesArgs.length).filter(i -> i % 2 == 0)
            .mapToObj(i -> (Class) typesArgs[i]).toArray(Class[]::new);
        Object[] args = IntStream.range(0, typesArgs.length).filter(i -> i % 2 == 1)
            .mapToObj(i -> typesArgs[i]).toArray(Object[]::new);
        Method method = object.getClass().getMethod(methodName, types);
        return method.invoke(object, args);
    }

    // Agent class doesn't have methods to stop agent
}
