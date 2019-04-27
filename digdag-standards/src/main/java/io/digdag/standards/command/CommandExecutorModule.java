package io.digdag.standards.command;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.digdag.spi.CommandExecutor;
import io.digdag.standards.command.kubernetes.DefaultKubernetesClientFactory;
import io.digdag.standards.command.kubernetes.KubernetesClientFactory;

public class CommandExecutorModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(CommandExecutor.class).to(KubernetesCommandExecutor.class).in(Scopes.SINGLETON);
        binder.bind(KubernetesClientFactory.class).to(DefaultKubernetesClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(SimpleCommandExecutor.class).in(Scopes.SINGLETON);
        binder.bind(DockerCommandExecutor.class).in(Scopes.SINGLETON);
    }
}
