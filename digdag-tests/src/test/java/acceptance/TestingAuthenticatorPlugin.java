package acceptance;

import com.google.inject.Binder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AuthenticatedUser;
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
        private final ConfigFactory cf;

        @Inject
        public TestingAuthenticatorFactory(Config systemConfig, ConfigFactory cf)
        {
            this.systemConfig = systemConfig;
            this.cf = cf;
        }

        public String getType()
        {
            return "testing";
        }

        public Authenticator newAuthenticator()
        {
            String headerName = systemConfig.get("server.authenticator.testing.header", String.class);
            return new TestingAuthenticator(cf, headerName);
        }
    }

    static class TestingAuthenticator
            implements Authenticator
    {
        private final ConfigFactory cf;
        private final String headerName;

        public TestingAuthenticator(ConfigFactory cf, String headerName)
        {
            this.cf = cf;
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
                AuthenticatedUser user = AuthenticatedUser.builder()
                    .siteId(0)
                    .userInfo(cf.create())
                    .userContext(cf.create())
                    .build();
                return Result.accept(user);
            }
        }
    }
}
