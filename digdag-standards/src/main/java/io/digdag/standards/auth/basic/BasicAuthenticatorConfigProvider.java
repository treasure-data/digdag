package io.digdag.standards.auth.basic;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;

import java.util.Optional;

public class BasicAuthenticatorConfigProvider
        implements Provider<Optional<BasicAuthenticatorConfig>>
{
    private final Config systemConfig;

    @Inject
    public BasicAuthenticatorConfigProvider(Config systemConfig)
    {
        this.systemConfig = systemConfig;
    }

    @Override
    public Optional<BasicAuthenticatorConfig> get()
    {
        try {
            return Optional.of(BasicAuthenticatorConfig.builder()
                    .username(systemConfig.getOptional("basicauth.username", String.class).get())
                    .password(systemConfig.getOptional("basicauth.password", String.class).get())
                    .isAdmin(systemConfig.getOptional("basicauth.admin", Boolean.class).or(false))
                    .build()
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}

