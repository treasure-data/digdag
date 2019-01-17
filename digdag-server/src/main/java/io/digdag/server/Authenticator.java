package io.digdag.server;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import io.digdag.client.config.Config;
import io.digdag.spi.AuthenticatedUser;
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
            return accept(siteId, Optional.absent(), Optional.absent());
        }

        static Result accept(int siteId, Config userInfo)
        {
            return accept(siteId, Optional.fromNullable(userInfo), Optional.absent());
        }

        static Result accept(int siteId, Config userInfo, AuthenticatedUser user)
        {
            return accept(siteId, Optional.fromNullable(userInfo), Optional.fromNullable(user));
        }

        static Result accept(int siteId, Optional<Config> userInfo, Optional<AuthenticatedUser> user)
        {
            return ImmutableResult.builder()
                    .siteId(siteId)
                    .userInfo(userInfo)
                    .authenticatedUser(user)
                    .build();
        }

        static Result reject(String message)
        {
            return ImmutableResult.builder()
                    .siteId(0)
                    .errorMessage(message)
                    .userInfo(Optional.absent())
                    .authenticatedUser(Optional.absent())
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

        Optional<Supplier<Map<String, String>>> getSecrets();

        Optional<AuthenticatedUser> getAuthenticatedUser();

        static Builder builder() {
            return ImmutableResult.builder();
        }

        interface Builder {
            Builder siteId(int siteId);
            Builder userInfo(Config userInfo);
            Builder secrets(Supplier<Map<String, String>> secrets);
            Builder isAdmin(boolean admin);
            Builder errorMessage(String errorMessage);
            Builder authenticatedUser(AuthenticatedUser user);
            Result build();
        }
    }

    Result authenticate(ContainerRequestContext requestContext);
}
