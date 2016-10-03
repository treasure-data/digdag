package io.digdag.guice.rs.server.jmx;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;

import org.weakref.jmx.guice.MBeanModule;

public class JmxModule
    extends AbstractModule
{
    @Override
    public void configure()
    {
        install(new MBeanModule());
        binder().bind(MBeanServer.class).toInstance(ManagementFactory.getPlatformMBeanServer());
        binder().bind(JmxAgent.class).asEagerSingleton();
    }
}
