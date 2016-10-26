package io.digdag.standards.operator.gcp;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.base.Optional;
import com.treasuredata.client.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;

class Gcp
{
    private static final Logger logger = LoggerFactory.getLogger(Gcp.class);

    static Proxy proxy(Optional<ProxyConfig> proxyConfig)
    {
        if (!proxyConfig.isPresent()) {
            return Proxy.NO_PROXY;
        }

        ProxyConfig cfg = proxyConfig.get();
        InetSocketAddress address = new InetSocketAddress(cfg.getHost(), cfg.getPort());
        Proxy proxy = new Proxy(Proxy.Type.HTTP, address);

        // TODO: support authenticated proxying
        Optional<String> user = cfg.getUser();
        Optional<String> password = cfg.getPassword();
        if (user.isPresent() || password.isPresent()) {
            logger.warn("Authenticated proxy is not supported");
        }

        return proxy;
    }

    static HttpTransport transport(Optional<ProxyConfig> proxyConfig)
    {
        return new NetHttpTransport.Builder()
                .setProxy(proxy(proxyConfig))
                .build();
    }

    static boolean isDeterministicException(GoogleJsonResponseException e)
    {
        return e.getStatusCode() / 100 == 4;
    }
}
