package io.digdag.server.auth;

import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.Authenticator;
import javax.ws.rs.container.ContainerRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoAuthenticator
    implements Authenticator
{
    private static final Logger logger = LoggerFactory.getLogger(NoAuthenticator.class);

    private final ConfigFactory cf;

    public NoAuthenticator(ConfigFactory cf)
    {
        logger.debug("No authentication is configured. No authentication is required to call REST API.");
        this.cf = cf;
    }

    @Override
    public Authenticator.Result authenticate(ContainerRequestContext requestContext)
    {
        AuthenticatedUser user = AuthenticatedUser.builder()
            .siteId(0)
            .userInfo(cf.create())
            .userContext(cf.create())
            .build();
        return Result.accept(user);
    }
}
