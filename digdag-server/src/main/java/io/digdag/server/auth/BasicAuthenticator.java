package io.digdag.server.auth;

import io.digdag.spi.Authenticator;
import javax.ws.rs.container.ContainerRequestContext;
import org.jboss.resteasy.util.BasicAuthHelper;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class BasicAuthenticator
    implements Authenticator
{
    private final String username;
    private final String password;

    public BasicAuthenticator(String username, String password)
    {
        this.username = username;
        this.password = password;
    }

    @Override
    public Authenticator.Result authenticate(ContainerRequestContext requestContext)
    {
        String authHeader = requestContext.getHeaderString(AUTHORIZATION);
        if (authHeader == null) {
            return Result.reject("Missing Authorization header");
        }

        String[] parsedHeader = BasicAuthHelper.parseHeader(authHeader);

        if (parsedHeader[0].equals(username) && parsedHeader[1].equals(password)) {
            return Result.accept(0);
        }
        else {
            return Result.reject("unauthorized");
        }
    }
}
