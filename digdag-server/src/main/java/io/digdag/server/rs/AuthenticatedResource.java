package io.digdag.server.rs;

import com.google.common.base.Supplier;
import io.digdag.client.config.Config;
import io.digdag.spi.AuthenticatedUser;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import java.util.Map;

public abstract class AuthenticatedResource
{
    @Context
    protected HttpServletRequest request;

    protected int getSiteId()
    {
        // siteId is set by JwtAuthInterceptor
        // TODO validate before causing NPE. Improve guice-rs to call @PostConstruct
        return getAuthenticatedUser().getSiteId();
    }

    protected Config getUserInfo()
    {
        return getAuthenticatedUser().getUserInfo();
    }

    protected AuthenticatedUser getAuthenticatedUser()
    {
        return (AuthenticatedUser) request.getAttribute("authenticatedUser");
    }

    /**
     * Get request context default secrets.
     */
    @SuppressWarnings("unchecked")
    protected Supplier<Map<String, String>> getSecrets()
    {
        return (Supplier<Map<String, String>>) request.getAttribute("secrets");
    }
}
