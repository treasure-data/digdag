package io.digdag.server;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.plugin.PluginSet;
import io.digdag.spi.Authenticator;
import io.digdag.spi.AuthenticatorFactory;
import java.util.Set;
import java.util.stream.Stream;

class AuthenticatorProvider
    implements Provider<Authenticator>
{
    private final Authenticator authenticator;

    @Inject
    public AuthenticatorProvider(Set<AuthenticatorFactory> injectedFactories, PluginSet.WithInjector pluginSet, Config systemConfig)
    {
        String name = systemConfig.get("server.authenticator.type", String.class, "basic");

        Stream<AuthenticatorFactory> candidates = Stream.concat(
                // Search from PluginSet first
                pluginSet.getServiceProviders(AuthenticatorFactory.class).stream(),
                // Then fallback to statically-injected authenticators
                injectedFactories.stream());

        AuthenticatorFactory factory = candidates
            .filter(candidate -> candidate.getType().equals(name))
            .findFirst()
            .orElseThrow(() -> new ConfigException("Configured authenticator name is not found: " + name));

        this.authenticator = factory.newAuthenticator();
    }

    @Override
    public Authenticator get()
    {
        return authenticator;
    }
}
