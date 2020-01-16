package io.digdag.server.auth;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Authenticator;
import io.digdag.spi.AuthenticatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthenticatorFactory
        implements AuthenticatorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(BasicAuthenticatorFactory.class);

    private final Config systemConfig;

    @Inject
    public BasicAuthenticatorFactory(Config systemConfig)
    {
        this.systemConfig = systemConfig;
    }

    @Override
    public String getType()
    {
        return "basic";
    }

    @Override
    public Authenticator newAuthenticator()
    {
        if (systemConfig.has("basicauth.username")) {
            logger.warn("Setting basicauth.username or basicauth.password works but is deprecated. It is going to be removed in a near future version. Use server.authenticator.basic.username and server.authenticator.basic.password instead.");
            return new BasicAuthenticator(
                    systemConfig.get("basicauth.username", String.class),
                    systemConfig.get("basicauth.password", String.class));
        }
        else if (systemConfig.has("server.authenticator.basic.username")) {
            return new BasicAuthenticator(
                    systemConfig.get("server.authenticator.basic.username", String.class),
                    systemConfig.get("server.authenticator.basic.password", String.class));
        }
        else {
            return new NoAuthenticator();
        }
    }
}
