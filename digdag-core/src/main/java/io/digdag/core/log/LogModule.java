package io.digdag.core.log;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.LogServerFactory;

public class LogModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(LogServerManager.class).in(Scopes.SINGLETON);

        Multibinder<LogServerFactory> logServerBinder = Multibinder.newSetBinder(binder, LogServerFactory.class);
        logServerBinder.addBinding().to(NullLogServerFactory.class).in(Scopes.SINGLETON);
        logServerBinder.addBinding().to(LocalFileLogServerFactory.class).in(Scopes.SINGLETON);
    }
}
