package io.digdag.standards;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.treasuredata.client.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.jboss.resteasy.util.Encode.decode;

public class Proxies
{
    private static final Logger logger = LoggerFactory.getLogger(Proxies.class);

    @VisibleForTesting
    public static Optional<ProxyConfig> proxyConfigFromEnv(String scheme, Map<String, String> env)
    {
        // Configuring http proxying using environment variables is not really standardized and
        // the actual use and interpretation of the environment variables vary from tool to tool.
        // For example, curl only respects https_proxy/HTTPS_PROXY for https requests whereas
        // the ruby Net::HTTP client only respects http_proxy/HTTP_PROXY.

        // We choose to be maximally lenient and respect all of the above in a hopefully sane order of preference.

        List<String> preference;
        switch (scheme) {
            case "http":
                preference = ImmutableList.of("http_proxy", "HTTP_PROXY");
                break;
            case "https":
                preference = ImmutableList.of("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY");
                break;
            default:
                throw new IllegalArgumentException("Unsupported scheme: " + scheme);
        }

        String proxyEnvVarName = null;
        String var = null;
        for (String name : preference) {
            var = env.getOrDefault(name, "").trim();
            if (!var.isEmpty()) {
                proxyEnvVarName = name;
                break;
            }
        }

        if (proxyEnvVarName == null) {
            return Optional.absent();
        }

        URI uri;
        try {
            uri = new URI(var);
        }
        catch (URISyntaxException e) {
            logger.warn("Failed to parse environment variable, ignoring: {}={}", proxyEnvVarName, var, e);
            return Optional.absent();
        }

        ProxyConfig.ProxyConfigBuilder builder = new ProxyConfig.ProxyConfigBuilder();

        builder.setHost(uri.getHost());

        if (uri.getPort() != -1) {
            builder.setPort(uri.getPort());
        }

        if (uri.getRawUserInfo() != null) {
            String userInfo = uri.getRawUserInfo();
            int colonIndex = userInfo.indexOf(':');
            if (colonIndex == -1) {
                builder.setUser(decode(userInfo));
            }
            else {
                String user = userInfo.substring(0, colonIndex);
                String pass = userInfo.substring(colonIndex + 1, userInfo.length());
                builder.setUser(decode(user));
                builder.setPassword(decode(pass));
            }
        }

        if ("https".equals(uri.getScheme())) {
            builder.useSSL(true);
        }

        return Optional.of(builder.createProxyConfig());
    }
}
