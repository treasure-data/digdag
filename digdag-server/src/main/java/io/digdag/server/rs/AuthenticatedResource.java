package io.digdag.server.rs;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

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

    protected Config getUserInfo()
    {
        return (Config) request.getAttribute("userInfo");
    }
}
