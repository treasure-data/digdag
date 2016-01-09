package io.digdag.guice.rs;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import javax.servlet.Servlet;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.internal.UniqueAnnotations;

public abstract class GuiceRsModule
        extends AbstractModule
{
    protected ServletBindingBuilder bindServlet(Class<Servlet> servlet)
    {
        Annotation annotation = UniqueAnnotations.create();
        Key<Servlet> servletKey = Key.get(servlet, UniqueAnnotations.create());
        binder().bind(servletKey).to(servlet).in(Scopes.SINGLETON);
        return new ServletBindingBuilder(binder(), servletKey);
    }

    protected ApplicationBindingBuilder bindApplication()
    {
        return new ApplicationBindingBuilder(binder());
    }

    private static abstract class AbstractServletBindingBuilder
        <T extends AbstractServletBindingBuilder, Initializer extends GuiceRsServletInitializer>
    {
        protected final Initializer initializer;

        AbstractServletBindingBuilder(Binder binder, Initializer initializer)
        {
            this.initializer = initializer;
            binder.bind(GuiceRsServletInitializer.class).annotatedWith(UniqueAnnotations.create()).toInstance(initializer);
        }

        @SuppressWarnings("unchecked")
        public T matches(String... urlPatterns)
        {
            for (String urlPattern : urlPatterns) {
                initializer.addMapping(urlPattern);
            }
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T withName(String name)
        {
            initializer.setName(name);
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T withInitParameter(String key, String value)
        {
            initializer.setInitParameter(key, value);
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T withAsyncSupported(boolean asyncSupported)
        {
            initializer.setAsyncSupported(asyncSupported);
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        public T withRunAsRole(String runAsRole)
        {
            initializer.setRunAsRole(runAsRole);
            return (T) this;
        }
    }

    public static class ServletBindingBuilder
            extends AbstractServletBindingBuilder<ServletBindingBuilder, ServletInitializer>
    {
        public ServletBindingBuilder(Binder binder, Key<Servlet> servletKey)
        {
            super(binder, new ServletInitializer(servletKey));
        }
    }

    public static class ApplicationBindingBuilder
            extends AbstractServletBindingBuilder<ApplicationBindingBuilder, ApplicationInitializer>
    {
        private final Binder binder;

        public ApplicationBindingBuilder(Binder binder)
        {
            super(binder, new ApplicationInitializer());
            this.binder = binder;
            withAsyncSupported(true);
        }

        @SuppressWarnings("unchecked")
        public ApplicationBindingBuilder withResource(Class<?>... annotatedClass)
        {
            for (Class<?> clazz : annotatedClass) {
                Key<Object> key = Key.get((Class<Object>) clazz, UniqueAnnotations.create());
                binder.bind(key).to(clazz);
                initializer.addResource(key, clazz);
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> ApplicationBindingBuilder withResource(Class<T> annotatedClass, Provider<? extends T> resourceProvider)
        {
            Key<T> key = Key.get(annotatedClass, UniqueAnnotations.create());
            binder.bind(key).toProvider(resourceProvider);
            initializer.addResource((Key<Object>) key, annotatedClass);
            return this;
        }

        @SuppressWarnings("unchecked")
        public ApplicationBindingBuilder withProvider(Class<?> annotatedClass)
        {
            Key<Object> key = Key.get((Class<Object>) annotatedClass, UniqueAnnotations.create());
            binder.bind(key).to(annotatedClass);
            initializer.addProvider(key);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> ApplicationBindingBuilder withProvider(Class<T> annotatedClass, Provider<? extends T> providerProvider)
        {
            Key<T> key = Key.get(annotatedClass, UniqueAnnotations.create());
            binder.bind(key).toProvider(providerProvider);
            initializer.addProvider((Key<Object>) key);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> ApplicationBindingBuilder withProvider(Class<T> annotatedClass, Class<? extends Provider<? extends T>> providerProvider)
        {
            Key<T> key = Key.get(annotatedClass, UniqueAnnotations.create());
            binder.bind(key).toProvider(providerProvider);
            initializer.addProvider((Key<Object>) key);
            return this;
        }
    }

    private static class ServletInitializer
            extends GuiceRsServletInitializer
    {
        private final Key<Servlet> servletKey;

        public ServletInitializer(Key<Servlet> servletKey)
        {
            this.servletKey = servletKey;
        }

        @Override
        protected Servlet initializeServlet(Injector injector)
        {
            return injector.getInstance(servletKey);
        }
    }

    private static class ApplicationInitializer
            extends GuiceRsServletInitializer
    {
        protected final Map<Key<Object>, Class<?>> resources = new HashMap<>();
        protected final Set<Key<Object>> providers = new HashSet<>();

        public void addResource(Key<Object> key, Class<?> annotatedClass)
        {
            this.resources.put(key, annotatedClass);
        }

        public void addProvider(Key<Object> key)
        {
            this.providers.add(key);
        }

        @Override
        protected Servlet initializeServlet(Injector injector)
        {
            return new GuiceRsApplicationServlet(injector, resources, providers);
        }
    }
}
