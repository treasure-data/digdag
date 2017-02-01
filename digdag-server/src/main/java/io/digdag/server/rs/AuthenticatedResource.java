package io.digdag.server.rs;

import com.google.common.base.Supplier;
import io.digdag.client.config.Config;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Context;
import static com.google.common.base.Strings.isNullOrEmpty;

public abstract class AuthenticatedResource
{
    @Context
    protected HttpServletRequest request;

    // Request attributes are set set by io.digdag.server.AuthRequestFilter.

    protected int getSiteId()
    {
        // TODO validate before causing NPE. Improve guice-rs to call @PostConstruct
        return (int) request.getAttribute("siteId");
    }

    protected Config getUserInfo()
    {
        return (Config) request.getAttribute("userInfo");
    }

    protected boolean isSuperAgent()
    {
        return (boolean) request.getAttribute("superAgent");
    }

    protected int getAgentSiteId()
    {
        if (isSuperAgent()) {
            String val = request.getHeader("X-Digdag-Site-Id");
            if (!isNullOrEmpty(val)) {
                try {
                    return Integer.valueOf(val);
                }
                catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("X-Digdag-Site-Id must be an integer");
                }
            }
        }
        return getSiteId();
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
