package io.digdag.core.agent;

import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.digdag.spi.Notifier;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.CommandLogger;

public class AgentModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(AgentId.class).toProvider(AgentIdProvider.class).in(Scopes.SINGLETON);

        binder.bind(ConfigEvalEngine.class).in(Scopes.SINGLETON);
        binder.bind(TemplateEngine.class).to(ConfigEvalEngine.class).in(Scopes.SINGLETON);
        binder.bind(HttpNotificationSender.class);
        binder.bind(MailNotificationSender.class);
        binder.bind(ShellNotificationSender.class);
        binder.bind(Notifier.class).to(DefaultNotifier.class).in(Scopes.SINGLETON);

        // log
        binder.bind(CommandLogger.class).to(TaskContextCommandLogger.class).in(Scopes.SINGLETON);
    }
}
