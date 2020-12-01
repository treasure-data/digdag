package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;
import io.digdag.spi.CommandLogger;
import io.digdag.standards.command.kubernetes.KubernetesClientFactory;

public class KubernetesCommandExecutorFactory
    implements CommandExecutorFactory
{
    private final Config systemConfig;
    private final KubernetesClientFactory kubernetesClientFactory;
    private final DockerCommandExecutor docker;
    private final StorageManager storageManager;
    private final ProjectArchiveLoader projectArchiveLoader;
    private final CommandLogger clog;

    @Inject
    public KubernetesCommandExecutorFactory(
            final Config systemConfig,
            final KubernetesClientFactory kubernetesClientFactory,
            final StorageManager storageManager,
            final ProjectArchiveLoader projectArchiveLoader,
            final CommandLogger clog)
    {
        this.systemConfig = systemConfig;
        this.kubernetesClientFactory = kubernetesClientFactory;
        this.docker = new DockerCommandExecutor(clog, new SimpleCommandExecutor(clog));
        this.storageManager = storageManager;
        this.projectArchiveLoader = projectArchiveLoader;
        this.clog = clog;
    }

    @Override
    public String getType()
    {
        return "kubernetes";
    }

    @Override
    public CommandExecutor newCommandExecutor()
    {
        return new KubernetesCommandExecutor(
                systemConfig,
                kubernetesClientFactory,
                docker,
                storageManager,
                projectArchiveLoader,
                clog);
    }
}
