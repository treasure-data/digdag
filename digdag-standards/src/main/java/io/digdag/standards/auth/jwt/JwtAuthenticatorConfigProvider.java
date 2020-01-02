package io.digdag.standards.auth.jwt;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.api.RestApiKey;
import io.digdag.client.config.Config;

public class JwtAuthenticatorConfigProvider
        implements Provider<JwtAuthenticatorConfig>
{

    private final Config systemConfig;

    @Inject
    public JwtAuthenticatorConfigProvider(Config systemConfig)
    {
        this.systemConfig = systemConfig;
    }

    @Override
    public JwtAuthenticatorConfig get()
    {
        ImmutableJwtAuthenticatorConfig.Builder builder = JwtAuthenticatorConfig.builder();

        Optional<RestApiKey> apiKey = systemConfig.getOptional("server.apikey", RestApiKey.class);

        if (apiKey.isPresent()) {
            UserConfig user = UserConfig.builder()
                    .siteId(0)
                    .isAdmin(true)
                    .apiKey(apiKey.get())
                    .build();

            builder
                    .userMap(ImmutableMap.of(user.getApiKey().getIdString(), user))
                    .isAllowPublicAccess(false);
        }
        else {
            builder
                    .userMap(ImmutableMap.of())
                    .isAllowPublicAccess(true);
        }

        return builder.build();
    }
}
