package io.digdag.standards.operator.gcp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.base.Optional;
import com.treasuredata.client.ProxyConfig;
import io.digdag.core.Environment;
import io.digdag.standards.Proxies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

abstract class BaseGcpClient<CLIENT extends AbstractGoogleJsonClient>
        implements AutoCloseable
{
    private static Logger logger = LoggerFactory.getLogger(BaseGcpClient.class);

    protected final CLIENT client;
    private final HttpTransport transport;

    BaseGcpClient(GoogleCredential credential, Optional<ProxyConfig> proxyConfig)
    {
        this.transport = Gcp.transport(proxyConfig);
        JsonFactory jsonFactory = new JacksonFactory();
        this.client = client(credential, transport, jsonFactory);
    }

    protected abstract CLIENT client(GoogleCredential credential, HttpTransport transport, JsonFactory jsonFactory);

    @Override
    public void close()
    {
        try {
            transport.shutdown();
        }
        catch (IOException e) {
            logger.warn("Error shutting down client", e);
        }
    }

    abstract static class Factory
    {
        protected final Optional<ProxyConfig> proxyConfig;

        protected Factory(@Environment Map<String, String> environment)
        {
            this.proxyConfig = Proxies.proxyConfigFromEnv("https", environment);
        }
    }
}
