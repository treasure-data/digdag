package io.digdag.server;

import com.google.inject.Inject;
import com.google.inject.Provider;
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
                    SessionResource.class
                )
            .withProvider(JacksonJsonProvider.class, JsonProviderProvider.class);
        binder().bind(ServerStarter.class).asEagerSingleton();
    }

    public static class JsonProviderProvider
            implements Provider<JacksonJsonProvider>
    {
        private final ObjectMapper mapper;

        @Inject
        public JsonProviderProvider(ObjectMapper mapper)
        {
            this.mapper = mapper;
        }

        public JacksonJsonProvider get()
        {
            return new JacksonJsonProvider(mapper);
        }
    }
}
