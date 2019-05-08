package io.digdag.server;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.digdag.client.DigdagVersion;
import io.digdag.client.config.ConfigException;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.crypto.SecretCryptoProvider;
import io.digdag.core.database.DatabaseSecretControlStoreManager;
import io.digdag.core.database.DatabaseSecretStoreManager;
import io.digdag.core.plugin.PluginSet;
import io.digdag.core.repository.ModelValidationException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.guice.rs.GuiceRsModule;
import io.digdag.server.ac.DefaultAccessController;
import io.digdag.server.rs.AdminResource;
import io.digdag.server.rs.AdminRestricted;
import io.digdag.server.rs.AttemptResource;
import io.digdag.server.rs.LogResource;
import io.digdag.server.rs.ProjectResource;
import io.digdag.server.rs.ScheduleResource;
import io.digdag.server.rs.SessionResource;
import io.digdag.server.rs.UiResource;
import io.digdag.server.rs.VersionResource;
import io.digdag.server.rs.WorkflowResource;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.Authenticator;
import io.digdag.spi.SecretControlStoreManager;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.util.Map;

import static io.digdag.guice.rs.GuiceRsServerRuntimeInfo.LISTEN_ADDRESS_NAME_ATTRIBUTE;

public class ServerModule
        extends GuiceRsModule
{
    private static final Logger logger = LoggerFactory.getLogger(ServerModule.class);
    private ServerConfig serverConfig;

    public ServerModule(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public void configure()
    {
        ApplicationBindingBuilder builder = bindApplication()
            .matches("/api/*")
            .addProvider(JacksonJsonProvider.class, JsonProviderProvider.class)
            .addProvider(AuthRequestFilter.class)
            .addProvider(CustomHeaderFilter.class)
            .addProvider(AdminRestrictedFilter.class)
            ;
        bindResources(builder);
        bindAuthorization();
        bindAuthenticator();
        bindExceptionhandlers(builder);
        bindSecrets();
        bindUiApplication();

        if (serverConfig.getEnableSwagger()) {
            enableSwagger(builder);
        }
    }

    protected void bindSecrets()
    {
        binder().bind(SecretCrypto.class).toProvider(SecretCryptoProvider.class).in(Scopes.SINGLETON);
        binder().bind(SecretStoreManager.class).to(DatabaseSecretStoreManager.class).in(Scopes.SINGLETON);
        binder().bind(SecretControlStoreManager.class).to(DatabaseSecretControlStoreManager.class);
    }

    protected void bindResources(ApplicationBindingBuilder builder)
    {
        builder.addResources(
                ProjectResource.class,
                WorkflowResource.class,
                ScheduleResource.class,
                SessionResource.class,
                AttemptResource.class,
                LogResource.class,
                VersionResource.class,
                AdminResource.class
            );
    }

    protected void bindAuthenticator()
    {
        binder().bind(Authenticator.class).toProvider(AuthenticatorProvider.class);
    }

    protected void bindAuthorization()
    {
        binder().bind(AccessController.class).to(DefaultAccessController.class);
    }

    protected void bindExceptionhandlers(ApplicationBindingBuilder builder)
    {
        builder
            .addProviderInstance(new GenericJsonExceptionHandler<AccessControlException>(Response.Status.FORBIDDEN) { })
            .addProviderInstance(new GenericJsonExceptionHandler<ResourceNotFoundException>(Response.Status.NOT_FOUND) { })
            .addProviderInstance(new GenericJsonExceptionHandler<StorageFileNotFoundException>(Response.Status.NOT_FOUND) { })
            .addProviderInstance(new GenericJsonExceptionHandler<ResourceConflictException>(Response.Status.CONFLICT) { })
            .addProviderInstance(new GenericJsonExceptionHandler<NotSupportedException>(Response.Status.BAD_REQUEST) { })
            .addProviderInstance(new GenericJsonExceptionHandler<IOException>(Response.Status.BAD_REQUEST) { })  // happens if input is not gzip
            .addProviderInstance(new GenericJsonExceptionHandler<ModelValidationException>(Response.Status.BAD_REQUEST) { })
            .addProviderInstance(new GenericJsonExceptionHandler<ConfigException>(Response.Status.BAD_REQUEST) { })
            .addProviderInstance(new GenericJsonExceptionHandler<IllegalArgumentException>(Response.Status.BAD_REQUEST) { })
            .addProviderInstance(new GenericJsonExceptionHandler<ResourceLimitExceededException>(Response.Status.BAD_REQUEST) { })
            ;
    }

    protected void bindUiApplication()
    {
        bindApplication()
            .matches("/*")
            .addResources(UiResource.class)
            ;
    }

    protected void enableSwagger(ApplicationBindingBuilder builder) {
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setTitle("Digdag");
        beanConfig.setDescription("Digdag server API");
        beanConfig.setVersion(DigdagVersion.buildVersion().toString());
        beanConfig.setResourcePackage(VersionResource.class.getPackage().getName());
        beanConfig.setScan();

        builder.addProvider(SwaggerSerializers.class)
                .addProvider(CorsFilter.class)
                .addResources(SwaggerApiListingResource.class);
        logger.info("swagger api enabled on: /api/swagger.{json,yaml}");
    }

    public static class JsonProviderProvider
            implements com.google.inject.Provider<JacksonJsonProvider>
    {
        private final ObjectMapper mapper;

        @Inject
        public JsonProviderProvider(ObjectMapper mapper)
        {
            this.mapper = mapper.copy();
            this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        public JacksonJsonProvider get()
        {
            return new JacksonJsonProvider(mapper);
        }
    }

    public static class AuthenticatorProvider
            implements com.google.inject.Provider<Authenticator>
    {
        private final Authenticator authenticator;

        @Inject
        public AuthenticatorProvider(PluginSet.WithInjector pluginSet, ServerConfig serverConfig)
        {
            List<Authenticator> authenticators = pluginSet.getServiceProviders(Authenticator.class);
            String configuredAuthenticatorClass = serverConfig.getAuthenticatorClass();

            for (Authenticator candidate : authenticators) {
                if (candidate.getClass().getName().equals(configuredAuthenticatorClass)) {
                    authenticator = candidate;
                    return;
                }
            }
            throw new IllegalArgumentException("Configured authenticatorClass not found: " + configuredAuthenticatorClass);
        }

        @Override
        public Authenticator get()
        {
            return authenticator;
        }
    }

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

    @Provider
    public static class CustomHeaderFilter implements ContainerResponseFilter
    {
        private final Map<String, String> headers;

        @Inject
        public CustomHeaderFilter(ServerConfig config)
        {
            this.headers = config.getHeaders();
        }

        @Override
        public void filter(
                ContainerRequestContext request,
                ContainerResponseContext response)
        {
            headers.forEach(response.getHeaders()::add);
        }
    }

    @Provider
    @AdminRestricted
    public static class AdminRestrictedFilter
            implements ContainerRequestFilter
    {
        @Context
        private HttpServletRequest request;

        @Inject
        public AdminRestrictedFilter()
        { }

        @Override
        public void filter(ContainerRequestContext requestContext)
                throws IOException
        {
            // Only allow requests on the admin interfaces
            Object listenAddressName = requestContext.getProperty(LISTEN_ADDRESS_NAME_ATTRIBUTE);
            if (listenAddressName == null || !listenAddressName.equals(ServerConfig.ADMIN_ADDRESS)) {
                throw new NotFoundException();
            }

            // Only allow admin users
            final AuthenticatedUser user = (AuthenticatedUser) request.getAttribute("authenticatedUser");
            if (user == null || !user.isAdmin()) {
                throw new ForbiddenException();
            }
        }
    }

    @Path("/api/swagger.{type:json|yaml}")
    public static class SwaggerApiListingResource extends ApiListingResource
    {

    }
}
