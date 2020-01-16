package acceptance;

import com.google.inject.Binder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Authenticator;
import io.digdag.spi.AuthenticatorFactory;
import io.digdag.spi.Plugin;
import javax.ws.rs.container.ContainerRequestContext;
import org.jboss.resteasy.util.BasicAuthHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class TestingAuthenticatorPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == AuthenticatorFactory.class) {
            return TestingAuthenticatorFactory.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    static class TestingAuthenticatorFactory
            implements AuthenticatorFactory
    {
        private final Config systemConfig;

        @Inject
        public TestingAuthenticatorFactory(Config systemConfig)
        {
            this.systemConfig = systemConfig;
        }

        public String getType()
        {
            return "testing";
        }

        public Authenticator newAuthenticator()
        {
            String headerName = systemConfig.get("server.authenticator.testing.header", String.class);
            return new TestingAuthenticator(headerName);
        }
    }

    static class TestingAuthenticator
            implements Authenticator
    {
        private final String headerName;

        public TestingAuthenticator(String headerName)
        {
            this.headerName = headerName;
        }

        @Override
        public Authenticator.Result authenticate(ContainerRequestContext requestContext)
        {
            String header = requestContext.getHeaderString(headerName);
            if (header == null) {
                return Result.reject("Unauthorized");
            }
            else {
                return Result.accept(0);
            }
        }
    }
}
