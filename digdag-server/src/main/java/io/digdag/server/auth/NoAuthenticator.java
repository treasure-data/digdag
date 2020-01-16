package io.digdag.server.auth;

import io.digdag.spi.Authenticator;
import javax.ws.rs.container.ContainerRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoAuthenticator
    implements Authenticator
{
    private static final Logger logger = LoggerFactory.getLogger(NoAuthenticator.class);

    public NoAuthenticator()
    {
        logger.debug("No authentication is configured. No authentication is required to call REST API.");
    }

    @Override
    public Authenticator.Result authenticate(ContainerRequestContext requestContext)
    {
        return Result.accept(0);
    }
}
