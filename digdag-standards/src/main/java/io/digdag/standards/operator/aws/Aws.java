package io.digdag.standards.operator.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.treasuredata.client.ProxyConfig;
import io.digdag.standards.Proxies;
import org.eclipse.jetty.http.HttpStatus;

import java.util.Map;

class Aws
{
    static boolean isDeterministicException(AmazonServiceException ex)
    {
        int statusCode = ex.getStatusCode();
        switch (statusCode) {
            case HttpStatus.TOO_MANY_REQUESTS_429:
            case HttpStatus.REQUEST_TIMEOUT_408:
                return false;
            default:
                return statusCode >= 400 && statusCode < 500;
        }
    }

    static void configureProxy(ClientConfiguration configuration, Optional<String> endpoint, Map<String, String> environment)
    {
        String scheme = (endpoint.isPresent() && endpoint.get().startsWith("http://"))
                ? "http"
                : "https";
        configureProxy(configuration, environment, scheme);
    }

    static void configureProxy(ClientConfiguration configuration, Map<String, String> environment, String scheme)
    {
        Optional<ProxyConfig> proxyConfig = Proxies.proxyConfigFromEnv(scheme, environment);
        if (proxyConfig.isPresent()) {
            configureProxy(configuration, proxyConfig.get());
        }
    }

    static void configureProxy(ClientConfiguration configuration, ProxyConfig proxyConfig)
    {
        configuration.setProxyHost(proxyConfig.getHost());
        configuration.setProxyPort(proxyConfig.getPort());
        Optional<String> user = proxyConfig.getUser();
        if (user.isPresent()) {
            configuration.setProxyUsername(user.get());
        }
        Optional<String> password = proxyConfig.getPassword();
        if (password.isPresent()) {
            configuration.setProxyPassword(password.get());
        }
    }

    @SafeVarargs
    static <T> Optional<T> first(Supplier<Optional<T>>... suppliers)
    {
        for (Supplier<Optional<T>> supplier : suppliers) {
            Optional<T> optional = supplier.get();
            if (optional.isPresent()) {
                return optional;
            }
        }
        return Optional.absent();
    }
}
