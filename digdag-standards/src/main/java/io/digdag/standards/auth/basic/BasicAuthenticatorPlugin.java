package io.digdag.standards.auth.basic;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import io.digdag.spi.Authenticator;
import io.digdag.spi.Plugin;

import java.util.Optional;

public class BasicAuthenticatorPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == Authenticator.class) {
            return BasicAuthenticator.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    @Override
    public <T> void configureBinder(Class<T> type, Binder binder)
    {
        binder.bind(new TypeLiteral<Optional<BasicAuthenticatorConfig>>(){}).toProvider(BasicAuthenticatorConfigProvider.class).asEagerSingleton();
    }
}
