package io.digdag.server;

import javax.ws.rs.core.Context;
import javax.servlet.http.HttpServletRequest;

public abstract class AuthenticatedResource
{
    @Context
    protected HttpServletRequest request;

    protected int getSiteId()
    {
        // siteId is set by JwtAuthInterceptor
        // TODO validate before causing NPE. Improve guice-rs to call @PostConstruct
        return (int) request.getAttribute("siteId");
    }
}
