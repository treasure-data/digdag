package io.digdag.guice.rs;

import java.util.Map;
import java.util.Set;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.spi.PropertyInjector;
import org.jboss.resteasy.spi.Registry;
import org.jboss.resteasy.spi.ResourceFactory;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.plugins.server.servlet.HttpServlet30Dispatcher;

public class GuiceRsApplicationServlet
        extends HttpServlet30Dispatcher
{
    private final Injector injector;
    private final Map<Key<Object>, Class<?>> resources;
    private final Set<Key<Object>> providers;

    public GuiceRsApplicationServlet(
            Injector injector,
            Map<Key<Object>, Class<?>> resources,
            Set<Key<Object>> providers)
    {
        this.injector = injector;
        this.resources = resources;
        this.providers = providers;
    }

    @Override
    public void init(ServletConfig servletConfig)
            throws ServletException
    {
        super.init(servletConfig);
        Registry registry = servletContainerDispatcher.getDispatcher().getRegistry();
        ResteasyProviderFactory providerFactory = servletContainerDispatcher.getDispatcher().getProviderFactory();

        for (Key<Object> key : providers) {
            providerFactory.registerProviderInstance(injector.getInstance(key));
        }

        for (Map.Entry<Key<Object>, Class<?>> pair : resources.entrySet()) {
            GuiceRsResourceFactory resourceFactory = new GuiceRsResourceFactory(injector.getProvider(pair.getKey()), pair.getValue());
            registry.addResourceFactory(resourceFactory);
        }
    }

    private static class GuiceRsResourceFactory
            implements ResourceFactory
    {
        private final Provider<Object> provider;
        private final Class<?> scannableClass;
        private PropertyInjector contextPropertyInjector;

        public GuiceRsResourceFactory(Provider<Object> provider, Class<?> scannableClass)
        {
            this.provider = provider;
            this.scannableClass = scannableClass;
        }

        @Override
        public Class<?> getScannableClass()
        {
            return scannableClass;
        }

        @Override
        public void registered(ResteasyProviderFactory factory)
        {
            contextPropertyInjector = factory.getInjectorFactory().createPropertyInjector(scannableClass, factory);
        }

        @Override
        public Object createResource(HttpRequest request, HttpResponse response, ResteasyProviderFactory factory)
        {
            Object resource = provider.get();
            contextPropertyInjector.inject(request, response, resource);
            return resource;
        }

        @Override
        public void requestFinished(HttpRequest request, HttpResponse response, Object resource)
        { }

        @Override
        public void unregistered()
        { }
    }
}
