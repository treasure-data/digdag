package io.digdag.standards.td;

import com.google.inject.Inject;
import com.treasuredata.client.TDClientConfig;
import io.digdag.client.DigdagClient;
import io.digdag.spi.DigdagClientConfigurator;

import javax.annotation.Nullable;

import java.net.URI;

public class TdDigdagClientConfigurator
        implements DigdagClientConfigurator
{
    private static final String DEFAULT_ENDPOINT = "https://api-workflow.treasuredata.com:443";

    private final boolean enabled = Boolean.parseBoolean(System.getProperty("io.digdag.standards.td.client-configurator.enabled", "false"));
    private final String endpoint = System.getProperty("io.digdag.standards.td.client-configurator.endpoint", DEFAULT_ENDPOINT);

    private final TDClientConfig clientConfig;

    @Inject
    public TdDigdagClientConfigurator(@Nullable TDClientConfig clientConfig)
    {
        this.clientConfig = clientConfig;
    }

    @Override
    public DigdagClient.Builder configureClient(DigdagClient.Builder builder)
    {
        if (!enabled || clientConfig == null) {
            return builder;
        }

        URI uri = URI.create(endpoint);

        builder.host(uri.getHost());
        builder.port((uri.getPort() != -1) ? uri.getPort() : defaultPort(uri.getScheme()));
        builder.ssl("https".equals(uri.getScheme()));

        if (clientConfig.apiKey.isPresent()) {
            // TODO: support other authorization value forms as well
            builder.header("Authorization", "TD1 " + clientConfig.apiKey.get());
        }

        return builder;
    }

    private int defaultPort(String scheme)
    {
        switch (scheme) {
            case "http":
                return 80;
            case "https":
                return 443;
        }
        throw new IllegalArgumentException("Unknown scheme: " + scheme);
    }
}
