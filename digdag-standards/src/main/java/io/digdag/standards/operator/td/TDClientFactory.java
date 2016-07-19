package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.common.annotations.VisibleForTesting;
import com.treasuredata.client.ProxyConfig;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.SecretProvider;

class TDClientFactory
{
    @VisibleForTesting
    static TDClientBuilder clientBuilderFromConfig(Config params, SecretProvider secrets)
    {
        TDClientBuilder builder = TDClient.newBuilder(false);

        SecretProvider proxySecrets = secrets.getSecrets("proxy");
        Config proxyConfig = params.getNestedOrGetEmpty("proxy");
        boolean proxyEnabled = proxySecrets.getSecretOptional("enabled").transform(Boolean::parseBoolean).or(() -> proxyConfig.get("enabled", Boolean.class, false));
        if (proxyEnabled) {
            builder.setProxy(proxyConfig(proxyConfig, proxySecrets));
        }

        String apikey = secrets.getSecretOptional("apikey").or(() -> params.get("apikey", String.class)).trim();
        if (apikey.isEmpty()) {
            throw new ConfigException("Parameter 'apikey' is empty");
        }
        
        return builder
                .setEndpoint(secrets.getSecretOptional("endpoint").or(() -> params.get("endpoint", String.class, "api.treasuredata.com")))
                .setUseSSL(secrets.getSecretOptional("use_ssl").transform(Boolean::parseBoolean).or(() -> params.get("use_ssl", boolean.class, true)))
                .setApiKey(apikey)
                .setRetryLimit(0)  // disable td-client's retry mechanism
                ;
    }

    static TDClient clientFromConfig(Config params, SecretProvider secrets)
    {
        return clientBuilderFromConfig(params, secrets).build();
    }

    private static ProxyConfig proxyConfig(Config config, SecretProvider secrets)
    {
        ProxyConfig.ProxyConfigBuilder builder = new ProxyConfig.ProxyConfigBuilder();

        Optional<String> host = secrets.getSecretOptional("host").or(config.getOptional("host", String.class));
        if (host.isPresent()) {
            builder.setHost(host.get());
        }

        Optional<Integer> port = secrets.getSecretOptional("port").transform(Integer::parseInt).or(config.getOptional("port", Integer.class));
        if (port.isPresent()) {
            builder.setPort(port.get());
        }

        Optional<String> user = secrets.getSecretOptional("user").or(config.getOptional("user", String.class));
        if (user.isPresent()) {
            builder.setUser(user.get());
        }

        Optional<String> password = secrets.getSecretOptional("password").or(config.getOptional("password", String.class));
        if (password.isPresent()) {
            builder.setPassword(password.get());
        }

        Optional<Boolean> useSsl = config.getOptional("use_ssl", Boolean.class);
        if (useSsl.isPresent()) {
            builder.useSSL(useSsl.get());
        }

        return builder.createProxyConfig();
    }
}
