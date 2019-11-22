package io.digdag.standards.operator.td;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.treasuredata.client.ProxyConfig;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.Proxies;
import io.digdag.standards.operator.td.TDOperator.SystemDefaultConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.jboss.resteasy.util.Encode.decode;

public class TDClientFactory implements BaseTDClientFactory
{

    private static Logger logger = LoggerFactory.getLogger(TDClientFactory.class);

    @Override
    public TDClient createClient(SystemDefaultConfig systemDefaultConfig, Map<String, String> env, Config params, SecretProvider secrets)
    {
        return clientFromConfig(systemDefaultConfig, env, params, secrets);
    }

    protected static TDClientBuilder clientBuilderFromConfig(SystemDefaultConfig systemDefaultConfig, Map<String, String> env, Config params, SecretProvider secrets)
    {
        TDClientBuilder builder = TDClient.newBuilder(false);

        boolean useSSL = secrets.getSecretOptional("use_ssl").transform(Boolean::parseBoolean).or(() -> params.get("use_ssl", boolean.class, true));
        String scheme = useSSL ? "https" : "http";

        SecretProvider proxySecrets = secrets.getSecrets("proxy");
        Config proxyConfig = params.getNestedOrGetEmpty("proxy");
        boolean proxyEnabled = proxySecrets.getSecretOptional("enabled").transform(Boolean::parseBoolean).or(() -> proxyConfig.get("enabled", Boolean.class, false));
        if (proxyEnabled) {
            builder.setProxy(proxyConfig(proxyConfig, proxySecrets));
        }
        else {
            Optional<ProxyConfig> config = Proxies.proxyConfigFromEnv(scheme, env);
            if (config.isPresent()) {
                builder.setProxy(config.get());
            }
        }

        Optional<String> apikey = secrets.getSecretOptional("apikey").transform(String::trim);
        if (!apikey.isPresent()) {
            throw new ConfigException("The 'td.apikey' secret is missing");
        }
        if (apikey.get().isEmpty()) {
            throw new ConfigException("The 'td.apikey' secret is empty");
        }

        return builder
                .setEndpoint(secrets.getSecretOptional("endpoint").or(() -> params.get("endpoint", String.class, systemDefaultConfig.getEndpoint())))
                .setUseSSL(useSSL)
                .setApiKey(apikey.get())
                .setRetryLimit(0)  // disable td-client's retry mechanism
                ;
    }

    @VisibleForTesting
    static TDClient clientFromConfig(SystemDefaultConfig systemDefaultConfig, Map<String, String> env, Config params, SecretProvider secrets)
    {
        return clientBuilderFromConfig(systemDefaultConfig, env, params, secrets).build();
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
