package io.digdag.server;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.NotSupportedException;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.digdag.guice.rs.GuiceRsModule;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.client.config.ConfigException;
import io.digdag.server.rs.RepositoryResource;
import io.digdag.server.rs.ScheduleResource;
import io.digdag.server.rs.SessionResource;
import io.digdag.server.rs.LogResource;
import io.digdag.server.rs.TempFileManager;

public class ServerModule
        extends GuiceRsModule
{
    @Override
    public void configure()
    {
        bindApplication()
            .matches("/api/*")
            .withResource(
                    RepositoryResource.class,
                    ScheduleResource.class,
                    SessionResource.class,
                    LogResource.class
                )
            .withProvider(JacksonJsonProvider.class, JsonProviderProvider.class)
            .withProvider(JwtAuthInterceptor.class)
            .withProviderInstance(new GenericJsonExceptionHandler<ResourceNotFoundException>(Response.Status.NOT_FOUND) { })
            .withProviderInstance(new GenericJsonExceptionHandler<ResourceConflictException>(Response.Status.CONFLICT) { })
            .withProviderInstance(new GenericJsonExceptionHandler<NotSupportedException>(Response.Status.BAD_REQUEST) { })
            .withProviderInstance(new GenericJsonExceptionHandler<ConfigException>(Response.Status.BAD_REQUEST) { })
            .withProviderInstance(new GenericJsonExceptionHandler<ConfigException>(Response.Status.BAD_REQUEST) { })
            .withProvider(CorsFilter.class);
        binder().bind(TempFileManager.class).in(Scopes.SINGLETON);
    }

    public static class JsonProviderProvider
            implements com.google.inject.Provider<JacksonJsonProvider>
    {
        private final ObjectMapper mapper;

        @Inject
        public JsonProviderProvider(ObjectMapper mapper)
        {
            this.mapper = mapper;
        }

        @Override
        public JacksonJsonProvider get()
        {
            return new JacksonJsonProvider(mapper);
        }
    }

    // TODO disable by option
    @Provider
    public static class CorsFilter implements ContainerResponseFilter
    {
        @Override
        public void filter(
                ContainerRequestContext request,
                ContainerResponseContext response)
        {
            response.getHeaders().add("Access-Control-Allow-Origin", "*");
            response.getHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization");
            response.getHeaders().add("Access-Control-Allow-Credentials", "true");
            response.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
            response.getHeaders().add("Access-Control-Max-Age", "1209600");
        }
    }

}
