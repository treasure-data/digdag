package io.digdag.server;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import com.google.inject.Inject;

@Provider
public class AuthRequestFilter
    implements ContainerRequestFilter
{
    private Authenticator auth;

    private final GenericJsonExceptionHandler<NotAuthorizedException> errorResultHandler;

    @Inject
    public AuthRequestFilter(Authenticator auth)
    {
        this.auth = auth;
        this.errorResultHandler = new GenericJsonExceptionHandler<NotAuthorizedException>(Response.Status.UNAUTHORIZED) { };
    }

    @Override
    public void filter(ContainerRequestContext requestContext)
    {
        if (requestContext.getUriInfo().getPath().equals("/api/version")) {
            return;
        }

        Authenticator.Result result = auth.authenticate(requestContext);
        if (result.isAccepted()) {
            requestContext.setProperty("siteId", result.getSiteId());
            requestContext.setProperty("userInfo", result.getUserInfo());
        }
        else {
            requestContext.abortWith(errorResultHandler.toResponse(result.getErrorMessage()));
        }
    }
}
