package io.digdag.server;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.digdag.client.config.ConfigException;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.crypto.SecretCryptoProvider;
import io.digdag.core.database.DatabaseSecretControlStoreManager;
import io.digdag.core.database.DatabaseSecretStoreManager;
import io.digdag.core.database.ThreadLocalTransactionManager;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ModelValidationException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.guice.rs.GuiceRsModule;
import io.digdag.guice.rs.server.undertow.UndertowServerInfo;
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
import io.digdag.spi.SecretControlStoreManager;
import io.digdag.spi.SecretStoreManager;
import io.digdag.spi.StorageFileNotFoundException;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;

import java.io.IOException;
import java.util.Map;

public class ServerModule
        extends GuiceRsModule
{
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
        bindAuthenticator();
        bindExceptionhandlers(builder);
        bindSecrets();
        bindUiApplication();
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
        binder().bind(Authenticator.class).to(JwtAuthenticator.class);
    }

    protected void bindExceptionhandlers(ApplicationBindingBuilder builder)
    {
        builder
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
        private final UndertowServerInfo serverInfo;

        @Context
        private HttpServletRequest httpServletRequest;

        @Inject
        public AdminRestrictedFilter(UndertowServerInfo serverInfo)
        {
            this.serverInfo = serverInfo;
        }

        @Override
        public void filter(ContainerRequestContext requestContext)
                throws IOException
        {
            // Only allow requests on the admin interfaces
            if (!serverInfo.getLocalAdminAddresses().stream().anyMatch(a ->
                            a.getPort() == httpServletRequest.getLocalPort() &&
                            a.getAddress().getHostAddress().equals(httpServletRequest.getLocalAddr()))) {
                throw new NotFoundException();
            }

            // Only allow admin users
            Boolean admin = (Boolean) httpServletRequest.getAttribute("admin");
            if (admin == null || !admin) {
                throw new ForbiddenException();
            }
        }
    }

    @Provider
    public static class DatabaseTransactionManager
        implements ReaderInterceptor
    {
        private final ThreadLocalTransactionManager transactionManager;

        @Inject
        public DatabaseTransactionManager(DataSource ds)
        {
            this.transactionManager = new ThreadLocalTransactionManager(ds);
        }

        @Override
        public Object aroundReadFrom(ReaderInterceptorContext context)
                throws IOException, WebApplicationException
        {
            try {
                LoggerFactory.getLogger(ServerModule.class).debug("DatabaseTransactionManager#aroundReadFrom: start");
                return transactionManager.begin(context::proceed);
            }
            catch (IOException | WebApplicationException e) {
                throw e;
            }
            catch (Exception e) {
                Throwables.propagate(e);
            }
            throw new RuntimeException("Shouldn't reach here");
        }
    }
}
