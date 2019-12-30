package io.digdag.server;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.Authenticator;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class AuthenticatorTest
{
    private static final ConfigFactory CF = new ConfigFactory(DigdagClient.objectMapper());

    @Test
    public void reject()
            throws Exception
    {
        Authenticator.Result rejection = Authenticator.Result.reject("foobar");
        assertThat(rejection.isAccepted(), is(false));
        assertThat(rejection.getErrorMessage(), is("foobar"));
        assertThat(rejection.isAdmin(), is(false));
    }

    @Test
    public void accept()
            throws Exception
    {
        {
            Authenticator.Result acceptance = Authenticator.Result.accept(17);
            assertThat(acceptance.isAccepted(), is(true));
            assertThat(acceptance.getSiteId(), is(17));
            assertThat(acceptance.getUserInfo(), is(Optional.absent()));
            assertThat(acceptance.isAdmin(), is(false));
        }

        {
            Config userinfo = CF.create();
            userinfo.set("foo" ,"bar");
            Authenticator.Result acceptance = Authenticator.Result.accept(17, userinfo);
            assertThat(acceptance.isAccepted(), is(true));
            assertThat(acceptance.getSiteId(), is(17));
            assertThat(acceptance.getUserInfo(), is(Optional.of(userinfo)));
            assertThat(acceptance.isAdmin(), is(false));
        }

        {
            Config userinfo = CF.create();
            userinfo.set("foo" ,"bar");
            Authenticator.Result acceptance = Authenticator.Result.accept(17, Optional.of(userinfo));
            assertThat(acceptance.isAccepted(), is(true));
            assertThat(acceptance.getSiteId(), is(17));
            assertThat(acceptance.getUserInfo(), is(Optional.of(userinfo)));
            assertThat(acceptance.isAdmin(), is(false));
        }
    }

    @Test
    public void admin()
            throws Exception
    {
        Authenticator.Result acceptance = Authenticator.Result.builder()
                .siteId(17)
                .isAdmin(true)
                .build();

        assertThat(acceptance.isAccepted(), is(true));
        assertThat(acceptance.getSiteId(), is(17));
        assertThat(acceptance.isAdmin(), is(true));
    }
}
