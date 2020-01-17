package io.digdag.spi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.AuthenticatedUser;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import org.immutables.value.Value;

import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

public interface Authenticator
{
    @Value.Immutable
    @Value.Style(visibility = PACKAGE)
    @JsonSerialize(as = ImmutableResult.class)
    @JsonDeserialize(as = ImmutableResult.class)
    interface Result
    {
        static Result accept(AuthenticatedUser user)
        {
            return accept(user, () -> ImmutableMap.of());
        }

        static Result accept(AuthenticatedUser user, Supplier<Map<String, String>> secrets)
        {
            return ImmutableResult.builder()
                    .secrets(secrets)
                    .authenticatedUser(user)
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
            return !getErrorMessage().isPresent();
        }

        @Value.Check
        default void checkNull()
        {
            if (isAccepted()) {
                // authenticated user
                Preconditions.checkState(getSecrets().isPresent() && getAuthenticatedUser().isPresent());
            }
            else {
                // error message
                Preconditions.checkState(!getSecrets().isPresent() || !getAuthenticatedUser().isPresent());
            }
        }

        Optional<String> getErrorMessage();

        Optional<Supplier<Map<String, String>>> getSecrets();

        Optional<AuthenticatedUser> getAuthenticatedUser();
    }

    Result authenticate(ContainerRequestContext requestContext);
}
