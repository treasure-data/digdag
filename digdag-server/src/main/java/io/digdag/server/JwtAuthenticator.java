package io.digdag.server;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.security.Key;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.JwtException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.api.RestApiKey;
import io.digdag.client.config.Config;

public class JwtAuthenticator
    implements Authenticator
{
    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticator.class);

    private final Map<String, UserConfig> userMap;
    private final boolean allowPublicAccess;

    @Inject
    public JwtAuthenticator(Config systemConfig)
    {
        Optional<RestApiKey> apiKey = systemConfig.getOptional("server.apikey", RestApiKey.class);

        if (apiKey.isPresent()) {
            UserConfig user = UserConfig.builder()
                .siteId(0)
                .apiKey(apiKey.get())
                .build();
            this.userMap = ImmutableMap.of(user.getApiKey().getIdString(), user);
            this.allowPublicAccess = false;
        }
        else {
            this.userMap = ImmutableMap.of();
            this.allowPublicAccess = true;
        }
    }

    @Override
    public Result authenticate(ContainerRequestContext requestContext)
    {
        int siteId;

        String auth = requestContext.getHeaderString("Authorization");
        if (auth == null) {
            if (allowPublicAccess) {
                // OK
                siteId = 0;
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
            }
            catch (JwtException ex) {
                logger.trace("Authentication failed: ", ex);
                return Result.reject("Authorization failed");
            }
        }

        return Result.accept(siteId);
    }
}
