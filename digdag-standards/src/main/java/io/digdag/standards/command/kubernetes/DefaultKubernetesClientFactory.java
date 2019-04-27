package io.digdag.standards.command.kubernetes;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;

public class DefaultKubernetesClientFactory
        implements KubernetesClientFactory
{
    @Override
    public KubernetesClient newClient(final KubernetesClientConfig kubernetesClientConfig)
    {
        final Config clientConfig = new ConfigBuilder()
                .withMasterUrl(kubernetesClientConfig.getMaster())
                .withNamespace(kubernetesClientConfig.getNamespace())
                .withCaCertData(kubernetesClientConfig.getCertsCaData())
                .withOauthToken(kubernetesClientConfig.getOauthToken())
                .build();
        return new DefaultKubernetesClient(kubernetesClientConfig,
                new io.fabric8.kubernetes.client.DefaultKubernetesClient(clientConfig));
    }
}
