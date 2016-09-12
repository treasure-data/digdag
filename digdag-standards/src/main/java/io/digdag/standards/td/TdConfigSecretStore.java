package io.digdag.standards.td;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import com.treasuredata.client.TDClientConfig;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;

import javax.annotation.Nullable;

import java.util.Map;

class TdConfigSecretStore
        implements SecretStore
{
    private final Map<String, String> secrets;

    private final boolean enabled = Boolean.parseBoolean(System.getProperty("io.digdag.standards.td.secrets.enabled", "true"));

    @Inject
    public TdConfigSecretStore(@Nullable TDClientConfig clientConfig)
    {
        this.secrets = (!enabled || clientConfig == null)
                ? ImmutableMap.of()
                : secrets(clientConfig);
    }

    private static ImmutableMap<String, String> secrets(TDClientConfig clientConfig)
    {
        ImmutableMap.Builder<String, String> secrets = ImmutableMap.builder();

        secrets.put("td.endpoint", clientConfig.endpoint);
        if (clientConfig.apiKey.isPresent()) {
            secrets.put("td.apikey", clientConfig.apiKey.get());
        }
        secrets.put("td.use_ssl", String.valueOf(clientConfig.useSSL));

        secrets.put("td.proxy.enabled", String.valueOf(clientConfig.proxy.isPresent()));
        if (clientConfig.proxy.isPresent()) {
            ProxyConfig proxy = clientConfig.proxy.get();
            secrets.put("td.proxy.host", proxy.getHost());
            secrets.put("td.proxy.port", String.valueOf(proxy.getPort()));
            Optional<String> user = proxy.getUser();
            if (user.isPresent()) {
                secrets.put("td.proxy.user", user.get());
            }
            Optional<String> password = proxy.getPassword();
            if (password.isPresent()) {
                secrets.put("td.proxy.password", password.get());
            }
        }

        return secrets.build();
    }

    @Override
    public Optional<String> getSecret(int projectId, String scope, String key)
    {
        if (!scope.equals(SecretScopes.PROJECT_DEFAULT)) {
            return Optional.absent();
        }
        return Optional.fromNullable(secrets.get(key));
    }
}
