package io.digdag.spi;

import io.digdag.client.config.Config;
import org.immutables.value.Value;

@Value.Immutable
public interface AuthenticatedUser
{
    int getSiteId();

    Config getUserInfo();

    Config getUserContext();

    static ImmutableAuthenticatedUser.Builder builder()
    {
        return ImmutableAuthenticatedUser.builder();
    }
}
