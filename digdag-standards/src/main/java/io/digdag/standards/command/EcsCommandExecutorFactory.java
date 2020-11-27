package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;
import io.digdag.spi.CommandLogger;
import io.digdag.standards.command.ecs.EcsClientFactory;

public class EcsCommandExecutorFactory
        implements CommandExecutorFactory
{
    private final Config systemConfig;
    private final EcsClientFactory ecsClientFactory;
    private final DockerCommandExecutor docker;
    private final StorageManager storageManager;
    private final ProjectArchiveLoader projectArchiveLoader;
    private final CommandLogger clog;

    @Inject
    public EcsCommandExecutorFactory(
            final Config systemConfig,
            final EcsClientFactory ecsClientFactory,
            DockerCommandExecutor docker, final StorageManager storageManager,
            final ProjectArchiveLoader projectArchiveLoader,
            final CommandLogger clog)
    {
        this.systemConfig = systemConfig;
        this.ecsClientFactory = ecsClientFactory;
        this.docker = docker;
        this.storageManager = storageManager;
        this.projectArchiveLoader = projectArchiveLoader;
        this.clog = clog;
    }

    @Override
    public String getType()
    {
        return "ecs";
    }

    @Override
    public CommandExecutor newCommandExecutor()
    {
        return new EcsCommandExecutor(
                systemConfig,
                ecsClientFactory,
                docker,
                storageManager,
                projectArchiveLoader,
                clog);
    }
}
