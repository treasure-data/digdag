package io.digdag.standards.auth.basic;

import com.google.inject.Inject;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Authenticator;
import io.digdag.spi.AuthenticatedUser;
import org.jboss.resteasy.util.BasicAuthHelper;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.Optional;

import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class BasicAuthenticator
    implements Authenticator
{
    private final Optional<BasicAuthenticatorConfig> config;
    private final ConfigFactory cf;

    @Inject
    public BasicAuthenticator(final ConfigFactory cf, Optional<BasicAuthenticatorConfig> config)
    {
        this.cf = cf;
        this.config = config;
    }

    @Override
    public Result authenticate(ContainerRequestContext requestContext)
    {
        if (!config.isPresent()) {
            throw new IllegalStateException("The BasicAuthenticator wasn't property configured.");
        }
        BasicAuthenticatorConfig basicAuthenticatorConfig = config.get();
        String username = basicAuthenticatorConfig.getUsername();
        String password = basicAuthenticatorConfig.getPassword();
        boolean isAdmin = basicAuthenticatorConfig.isAdmin();


        String authHeader = requestContext.getHeaderString(AUTHORIZATION);
        if (authHeader == null) {
            return Result.reject("Missing Authorization header");
        }

        String[] parsedHeader = BasicAuthHelper.parseHeader(authHeader);

        if (parsedHeader[0].equals(username) && parsedHeader[1].equals(password)) {
            return Result.builder()
                    .isAdmin(isAdmin)
                    .siteId(0)
                    .authenticatedUser(AuthenticatedUser.builder()
                          .siteId(0)
                          .isAdmin(isAdmin)
                          .userInfo(cf.create())
                          .userContext(cf.create())
                          .build())
                    .build();
        } else {
            return Result.reject("unauthorized");
        }
    }
}
