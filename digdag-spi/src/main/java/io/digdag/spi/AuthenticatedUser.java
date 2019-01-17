package io.digdag.spi;

import io.digdag.client.config.Config;
import org.immutables.value.Value;

@Value.Immutable
public interface AuthenticatedUser
{
    int getSiteId();

    Config getUserInfo();

    static AuthenticatedUser of(int siteId, Config userInfo)
    {
        return ImmutableAuthenticatedUser.builder()
                .siteId(siteId)
                .userInfo(userInfo)
                .build();
    }
}
