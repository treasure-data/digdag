package io.digdag.server;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.digdag.guice.rs.GuiceRsModule;

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
                    WorkflowResource.class
                )
            .withProvider(JacksonJsonProvider.class, JsonProviderProvider.class)
            .withProvider(CorsFilter.class);
        binder().bind(ServerStarter.class).asEagerSingleton();
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
