package io.digdag.standards.command;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.spi.CommandExecutor;
import io.digdag.standards.command.ecs.DefaultEcsClientFactory;
import io.digdag.standards.command.ecs.EcsClientFactory;

public class CommandExecutorModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(CommandExecutor.class).to(EcsCommandExecutor.class).in(Scopes.SINGLETON);
        binder.bind(EcsClientFactory.class).to(DefaultEcsClientFactory.class).in(Scopes.SINGLETON);
        //binder.bind(CommandExecutor.class).to(KubernetesCommandExecutor.class).in(Scopes.SINGLETON);
        //binder.bind(KubernetesClientFactory.class).to(DefaultKubernetesClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(SimpleCommandExecutor.class).in(Scopes.SINGLETON);
        binder.bind(DockerCommandExecutor.class).in(Scopes.SINGLETON);
    }
}
