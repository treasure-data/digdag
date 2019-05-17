package io.digdag.standards.command.kubernetes;

public interface KubernetesClientFactory
{
    KubernetesClient newClient(KubernetesClientConfig kubernetesClientConfig);
}
