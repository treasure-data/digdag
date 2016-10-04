package io.digdag.guice.rs;

import java.util.Map;
import java.util.Set;
import java.io.IOException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.annotation.HandlesTypes;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@HandlesTypes(GuiceRsBootstrap.class)
public class GuiceRsServletContainerInitializer
        implements ServletContainerInitializer
{
    @Override
    @SuppressWarnings("unchecked")
    public void onStartup(Set<Class<?>> classes, ServletContext ctx)
    {
        ctx.setAttribute(GuiceRsServletContainerInitializer.class.getName(), this);

        for (Class<?> bootstrap : classes) {
            if (GuiceRsBootstrap.class.isAssignableFrom(bootstrap)) {
                processBootstrap((Class<GuiceRsBootstrap>) bootstrap, ctx);
            }
            else {
                // TODO show warnings
            }
        }
    }

    private void processBootstrap(final Class<GuiceRsBootstrap> bootstrap, final ServletContext context)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        Module module;

        module = GuiceRsServerControlModule.fromInitParameter(context);
        if (module != null) {
            modules.add(module);
        }

        modules.add((binder) -> {
            binder.bind(GuiceRsBootstrap.class).to(bootstrap).asEagerSingleton();
            binder.bind(ServletContext.class).toInstance(context);
        });

        final Injector injector = Guice.createInjector(modules.build())
            .getInstance(GuiceRsBootstrap.class)
            .initialize(context);

        if (injector instanceof AutoCloseable) {
            context.addListener(new CloseableInjectorDestroyListener((AutoCloseable) injector));
        }

        injector.findBindingsByType(TypeLiteral.get(GuiceRsServletInitializer.class))
            .stream()
            .map(binding -> binding.getProvider().get())
            .forEach(initializer -> initializer.register(injector, context));

        // return 404 Not Found if all URL patterns don't match
        context.addServlet("default", new DefaultServlet())
            .addMapping("/");
    }

    private static class DefaultServlet extends HttpServlet
    {
        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException
        {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private static class CloseableInjectorDestroyListener
            implements ServletContextListener
    {
        private final AutoCloseable closeable;

        public CloseableInjectorDestroyListener(AutoCloseable closeable)
        {
            this.closeable = closeable;
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce)
        {
            try {
                closeable.close();
            }
            catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public void contextInitialized(ServletContextEvent sce)
        { }
    }
}
