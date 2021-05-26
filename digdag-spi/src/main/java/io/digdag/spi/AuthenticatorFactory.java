package io.digdag.spi;

public interface AuthenticatorFactory
{
    String getType();

    Authenticator newAuthenticator();
}
