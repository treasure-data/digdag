package io.digdag.server;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import javax.ws.rs.container.ContainerRequestContext;

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
            return ImmutableResult.builder()
                    .siteId(siteId)
                    .build();
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
                    .errorMessage(message)
                    .build();
        }

        default boolean isAccepted()
        {
            return getErrorMessage() == null;
        }

        int getSiteId();

        @Nullable
        String getErrorMessage();

        Optional<Config> getUserInfo();
    }

    Result authenticate(ContainerRequestContext requestContext);
}
