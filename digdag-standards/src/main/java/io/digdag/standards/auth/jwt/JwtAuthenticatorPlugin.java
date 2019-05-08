package io.digdag.standards.auth.jwt;

import com.google.inject.Binder;
import io.digdag.spi.Authenticator;
import io.digdag.spi.Plugin;

public class JwtAuthenticatorPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == Authenticator.class) {
            return JwtAuthenticator.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    @Override
    public <T> void configureBinder(Class<T> type, Binder binder)
    {
        binder.bind(JwtAuthenticatorConfig.class).toProvider(JwtAuthenticatorConfigProvider.class).asEagerSingleton();
    }
}
