package io.digdag.standards.command;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.standards.command.ecs.EcsClientFactory;
import io.digdag.standards.command.kubernetes.KubernetesClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutorProvider
        implements Provider<CommandExecutor>
{
    CommandExecutor executor;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Inject
    public CommandExecutorProvider(
            final Config systemConfig,
            final EcsClientFactory ecsClientFactory,
            final KubernetesClientFactory kubernetesClientFactory,
            final StorageManager storageManager,
            final ProjectArchiveLoader projectArchiveLoader,
            final CommandLogger clog)
    {
        SimpleCommandExecutor simple = new SimpleCommandExecutor(clog);
        DockerCommandExecutor docker = new DockerCommandExecutor(clog, simple);

        String executorType = systemConfig.get("agent.command_executor.type", String.class, "");
        logger.debug("Using command executor type: {}", executorType);

        switch (executorType) {
            case "ecs":
                executor = new EcsCommandExecutor(systemConfig, ecsClientFactory, docker, storageManager, projectArchiveLoader, clog);
                break;
            case "kubernetes":
                executor = new KubernetesCommandExecutor(systemConfig, kubernetesClientFactory, docker, storageManager, projectArchiveLoader, clog);
                break;
            case "":
                executor = docker;
                break;
            default:
                throw new ConfigException("Unsupported agent.command_executor.type : " + executorType);
        }
    }

    @Override
    public CommandExecutor get()
    {
        return executor;
    }
}

