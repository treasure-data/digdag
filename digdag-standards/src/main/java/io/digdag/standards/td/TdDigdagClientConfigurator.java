package io.digdag.standards.td;

import com.google.inject.Inject;
import com.treasuredata.client.TDClientConfig;
import io.digdag.client.DigdagClient;
import io.digdag.spi.DigdagClientConfigurator;

public class TdDigdagClientConfigurator
        implements DigdagClientConfigurator
{
    private final boolean enabled = Boolean.parseBoolean(System.getProperty("io.digdag.standards.td.client-configurator-enabled", "false"));

    private final TDClientConfig clientConfig;

    @Inject
    public TdDigdagClientConfigurator(TDClientConfig clientConfig)
    {
        this.clientConfig = clientConfig;
    }

    @Override
    public DigdagClient.Builder configureClient(DigdagClient.Builder builder)
    {
        if (!enabled) {
            return builder;
        }

        builder = DigdagClient.builder();
        builder.host("api-workflow.treasuredata.com");
        builder.ssl(true);

        if (clientConfig.apiKey.isPresent()) {
            // TODO: support other authorization value forms as well
            builder.header("Authorization", "TD1 " + clientConfig.apiKey.get());
        }

        return builder;
    }
}
