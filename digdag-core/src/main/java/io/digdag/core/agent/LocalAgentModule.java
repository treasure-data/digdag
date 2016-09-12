package io.digdag.core.agent;

import javax.annotation.PostConstruct;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.CommandLogger;

public class LocalAgentModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(AgentConfig.class).toProvider(AgentConfigProvider.class).in(Scopes.SINGLETON);
        binder.bind(WorkspaceManager.class).to(LocalWorkspaceManager.class).in(Scopes.SINGLETON);
        binder.bind(TaskCallbackApi.class).to(InProcessTaskCallbackApi.class).in(Scopes.SINGLETON);
        binder.bind(TaskServerApi.class).to(InProcessTaskServerApi.class).in(Scopes.SINGLETON);
        binder.bind(OperatorManager.class).in(Scopes.SINGLETON);

        // built-in operators
        Multibinder<OperatorFactory> taskExecutorBinder = Multibinder.newSetBinder(binder, OperatorFactory.class);
        taskExecutorBinder.addBinding().to(RequireOperatorFactory.class).in(Scopes.SINGLETON);
        taskExecutorBinder.addBinding().to(CallOperatorFactory.class).in(Scopes.SINGLETON);

        binder.bind(LocalAgentManager.class).asEagerSingleton();
    }
}
