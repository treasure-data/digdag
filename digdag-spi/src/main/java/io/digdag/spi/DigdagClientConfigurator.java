package io.digdag.spi;

import io.digdag.client.DigdagClient;

public interface DigdagClientConfigurator
{
    /**
     * Decorate and/or transform a {@link DigdagClient.Builder} with some configuration. May return the passed in builder instance or another instance entirely.
     */
    DigdagClient.Builder configureClient(DigdagClient.Builder builder);
}
