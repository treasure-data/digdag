package io.digdag.server;

import java.util.Map;
import java.security.Key;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import com.google.inject.Inject;
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

@Provider
public class JwtAuthInterceptor
    implements ContainerRequestFilter
{
    private static final Logger logger = LoggerFactory.getLogger(JwtAuthInterceptor.class);

    private final Map<String, UserConfig> userMap;
    private final boolean allowPublicAccess;
    private final GenericJsonExceptionHandler<NotAuthorizedException> errorResultHandler;

    @Inject
    public JwtAuthInterceptor(ServerConfig config)
    {
        ImmutableMap.Builder<String, UserConfig> builder = ImmutableMap.builder();
        for (UserConfig user : config.getApiKeyAuthUsers()) {
            builder.put(user.getApiKey().getIdString(), user);
        }
        this.userMap = builder.build();
        this.allowPublicAccess = config.getAllowPublicAccess();
        this.errorResultHandler = new GenericJsonExceptionHandler<NotAuthorizedException>(Response.Status.UNAUTHORIZED) { };
    }

    @Override
    public void filter(ContainerRequestContext requestContext)
    {
        int siteId;

        String auth = requestContext.getHeaderString("Authorization");
        if (auth == null) {
            if (allowPublicAccess) {
                // OK
                siteId = 0;
            }
            else {
                requestContext.abortWith(errorResultHandler.toResponse("Authorization is required"));
                return;
            }
        }
        else {
            String[] typeData = auth.split(" ", 2);
            if (typeData.length != 2) {
                requestContext.abortWith(errorResultHandler.toResponse("Invalid authorization header"));
                return;
            }
            if (!typeData[0].equals("Bearer")) {
                requestContext.abortWith(errorResultHandler.toResponse("Invalid authorization header"));
                return;
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
                requestContext.abortWith(errorResultHandler.toResponse("Authorization failed"));
                logger.trace("Authentication failed: ", ex);
                return;
            }
        }

        requestContext.setProperty("siteId", siteId);
    }
}
