package io.digdag.server;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.digdag.client.api.RestApiKey;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AuthenticatedUser;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.SigningKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.container.ContainerRequestContext;

import java.security.Key;
import java.util.Map;

public class JwtAuthenticator
    implements Authenticator
{
    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticator.class);

    private final ConfigFactory cf;
    private final Map<String, UserConfig> userMap;
    private final boolean allowPublicAccess;

    @Inject
    public JwtAuthenticator(Config systemConfig, final ConfigFactory cf)
    {
        Optional<RestApiKey> apiKey = systemConfig.getOptional("server.apikey", RestApiKey.class);

        if (apiKey.isPresent()) {
            UserConfig user = UserConfig.builder()
                .siteId(0)
                .isAdmin(true)
                .apiKey(apiKey.get())
                .build();
            this.userMap = ImmutableMap.of(user.getApiKey().getIdString(), user);
            this.allowPublicAccess = false;
        }
        else {
            this.userMap = ImmutableMap.of();
            this.allowPublicAccess = true;
        }
        this.cf = cf;
    }

    @Override
    public Result authenticate(ContainerRequestContext requestContext)
    {
        int siteId;
        boolean admin;

        String auth = requestContext.getHeaderString("Authorization");
        if (auth == null) {
            if (allowPublicAccess) {
                // OK
                siteId = 0;
                admin = true;
            }
            else {
                return Result.reject("Authorization is required");
            }
        }
        else {
            String[] typeData = auth.split(" ", 2);
            if (typeData.length != 2) {
                return Result.reject("Invalid authorization header");
            }
            if (!typeData[0].equals("Bearer")) {
                return Result.reject("Invalid authorization header");
            }
            String token = typeData[1];
            try {
                String subject = Jwts.parser().setSigningKeyResolver(new SigningKeyResolver() {
                    @Override
                    public Key resolveSigningKey(JwsHeader header, Claims claims)
                    {
                        Object keyType = header.get("knd");
                        if (keyType == null || !"ps1".equals(keyType)) {
                            throw new SignatureException("Invalid key type");
                        }
                        UserConfig user = userMap.get(claims.getSubject());
                        if (user == null) {
                            throw new SignatureException("Invalid subject");
                        }
                        return new SecretKeySpec(user.getApiKey().getSecret(), header.getAlgorithm());
                    }

                    @Override
                    public Key resolveSigningKey(JwsHeader header, String plaintext)
                    {
                        throw new SignatureException("Plain text JWT authorization header is not allowed");
                    }
                })
                .parseClaimsJws(token)
                .getBody()
                .getSubject();

                UserConfig user = userMap.get(subject);
                if (user == null) {
                    throw new SignatureException("Invalid subject");
                }

                siteId = user.getSiteId();
                admin = user.isAdmin();
            }
            catch (JwtException ex) {
                logger.trace("Authentication failed: ", ex);
                return Result.reject("Authorization failed");
            }
        }

        return Result.accept(
                createAuthenticatedUser(siteId, admin, requestContext),
                () -> ImmutableMap.of());
    }

    private AuthenticatedUser createAuthenticatedUser(final int siteId, final boolean admin, final ContainerRequestContext requestContext)
    {
        return AuthenticatedUser.builder()
                .siteId(siteId)
                .isAdmin(admin)
                .userInfo(cf.create())
                .userContext(cf.create())
                .build();
    }
}
