package io.digdag.core.notification;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.digdag.spi.NotificationSender;
import io.digdag.spi.Notifier;

public class NotificationModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(NotificationSender.class).annotatedWith(Names.named("http")).to(HttpNotificationSender.class);
        binder.bind(NotificationSender.class).annotatedWith(Names.named("mail")).to(MailNotificationSender.class);
        binder.bind(NotificationSender.class).annotatedWith(Names.named("shell")).to(ShellNotificationSender.class);
        binder.bind(Notifier.class).to(DefaultNotifier.class).in(Scopes.SINGLETON);
    }
}
