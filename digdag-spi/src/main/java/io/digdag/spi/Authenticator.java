package io.digdag.spi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.Map;

import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

public interface Authenticator
{
    @Value.Immutable
    @Value.Style(visibility = PACKAGE)
    @JsonSerialize(as = ImmutableResult.class)
    @JsonDeserialize(as = ImmutableResult.class)
    interface Result
    {
        static Result accept(int siteId)
        {
            return accept(siteId, Optional.absent());
        }

        static Result accept(int siteId, Config userInfo)
        {
            return accept(siteId, Optional.fromNullable(userInfo));
        }

        static Result accept(int siteId, Optional<Config> userInfo)
        {
            return ImmutableResult.builder()
                    .siteId(siteId)
                    .userInfo(userInfo)
                    .build();
        }

        static Result reject(String message)
        {
            return ImmutableResult.builder()
                    .siteId(0)
                    .errorMessage(message)
                    .userInfo(Optional.absent())
                    .build();
        }

        default boolean isAccepted()
        {
            return getErrorMessage() == null;
        }

        int getSiteId();

        @Value.Default
        default boolean isAdmin() {
            return false;
        }

        @Nullable
        String getErrorMessage();

        Optional<Config> getUserInfo();

        Optional<AuthenticatedUser> getAuthenticatedUser();

        Optional<Supplier<Map<String, String>>> getSecrets();

        static Builder builder() {
            return ImmutableResult.builder();
        }

        interface Builder {
            Builder siteId(int siteId);
            Builder userInfo(Config userInfo);
            Builder secrets(Supplier<Map<String, String>> secrets);
            Builder isAdmin(boolean admin);
            Builder authenticatedUser(AuthenticatedUser authenticatedUser);
            Builder errorMessage(String errorMessage);
            Result build();
        }
    }

    Result authenticate(ContainerRequestContext requestContext);
}
