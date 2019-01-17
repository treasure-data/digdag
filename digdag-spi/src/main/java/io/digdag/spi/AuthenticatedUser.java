package io.digdag.spi;

import io.digdag.client.config.Config;
import org.immutables.value.Value;

import java.util.Map;

@Value.Immutable
public interface AuthenticatedUser
{
    int getSiteId();

    Config getUserInfo();

    Map<String, String> getHeaders();

    static AuthenticatedUser of(int siteId, Config userInfo, Map<String, String> headers)
    {
        return ImmutableAuthenticatedUser.builder()
                .siteId(siteId)
                .userInfo(userInfo)
                .headers(headers)
                .build();
    }
}
