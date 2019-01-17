package io.digdag.spi;

import io.digdag.client.config.Config;
import org.immutables.value.Value;

@Value.Immutable
public interface AuthenticatedUser
{
    int getSiteId();

    boolean isAdmin();

    Config getUserInfo();

    // this context will not be stored on database instead of userInfo, which is stored as part of wf revision.
    Config getUserContext();

    static ImmutableAuthenticatedUser.Builder builder()
    {
        return ImmutableAuthenticatedUser.builder();
    }
}
