package io.digdag.server.auth;

import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.Authenticator;
import javax.ws.rs.container.ContainerRequestContext;
import org.jboss.resteasy.util.BasicAuthHelper;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class BasicAuthenticator
    implements Authenticator
{
    private final ConfigFactory cf;
    private final String username;
    private final String password;
    private final boolean admin;

    public BasicAuthenticator(ConfigFactory cf,
            String username, String password, boolean admin)
    {
        this.cf = cf;
        this.username = username;
        this.password = password;
        this.admin = admin;
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
            AuthenticatedUser user = AuthenticatedUser.builder()
                .siteId(0)
                .userInfo(cf.create())
                .userContext(cf.create())
                .isAdmin(admin)
                .build();
            return Result.accept(user);
        }
        else {
            return Result.reject("unauthorized");
        }
    }
}
