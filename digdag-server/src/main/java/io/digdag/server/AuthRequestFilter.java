package io.digdag.server;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.digdag.client.config.ConfigFactory;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

@Provider
@PreMatching
public class AuthRequestFilter
    implements ContainerRequestFilter
{
    private Authenticator auth;
    private ConfigFactory cf;

    private final GenericJsonExceptionHandler<NotAuthorizedException> errorResultHandler;

    @Inject
    public AuthRequestFilter(Authenticator auth, ConfigFactory cf)
    {
        this.auth = auth;
        this.cf = cf;
        this.errorResultHandler = new GenericJsonExceptionHandler<NotAuthorizedException>(Response.Status.UNAUTHORIZED) { };
    }

    @Override
    public void filter(ContainerRequestContext requestContext)
    {
        String method = requestContext.getMethod();

        if (method.equals("OPTIONS") || method.equals("TRACE")) {
            return;
        }

        if (requestContext.getUriInfo().getPath().equals("/api/version")) {
            return;
        }

        Authenticator.Result result = auth.authenticate(requestContext);
        if (result.isAccepted()) {
            requestContext.setProperty("secrets", result.getSecrets().get());
            requestContext.setProperty("authenticatedUser", result.getAuthenticatedUser().get());
        }
        else {
            requestContext.abortWith(errorResultHandler.toResponse(result.getErrorMessage().get()));
        }
    }
}
