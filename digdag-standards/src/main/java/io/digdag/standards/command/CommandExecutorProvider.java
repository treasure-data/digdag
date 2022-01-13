package io.digdag.standards.command;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.plugin.PluginSet;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;

import java.util.Set;
import java.util.stream.Stream;

public class CommandExecutorProvider
        implements Provider<CommandExecutor>
{
    private final CommandExecutor commandExecutor;

    @Inject
    public CommandExecutorProvider(Set<CommandExecutorFactory> injectedFactories, PluginSet.WithInjector pluginSet, Config systemConfig)
    {
        // Set ECS as default command executor type
        String executorName = systemConfig.get("agent.command_executor.type", String.class, "ecs");
        Stream<CommandExecutorFactory> candidates = Stream.concat(
                // Search from PluginSet first
                pluginSet.getServiceProviders(CommandExecutorFactory.class).stream(),
                // Then fallback to statically-injected commandExecutor
                injectedFactories.stream());

        CommandExecutorFactory factory = candidates
                .filter(candidate -> candidate.getType().equals(executorName))
                .findFirst()
                .orElseThrow(() -> new ConfigException("Configured commandExecutor name is not found: " + executorName));

        this.commandExecutor = factory.newCommandExecutor();
    }

    @Override
    public CommandExecutor get()
    {
        return commandExecutor;
    }
}
