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
import static org.hamcrest.MatcherAssert.assertThat;

public class AuthenticatorTest
{
    private static final ConfigFactory CF = new ConfigFactory(DigdagClient.objectMapper());

    @Test
    public void reject()
            throws Exception
    {
        Authenticator.Result rejection = Authenticator.Result.reject("foobar");
        assertThat(rejection.isAccepted(), is(false));
        assertThat(rejection.getErrorMessage(), is(Optional.of("foobar")));
        assertThat(rejection.getAuthenticatedUser(), is(Optional.absent()));
    }

    @Test
    public void accept()
            throws Exception
    {
        final Config userInfo = CF.create().set("k1", "v1");
        final Config userContext = CF.create().set("k2", "v2");

        final AuthenticatedUser user = AuthenticatedUser.builder()
                .siteId(17)
                .isAdmin(false)
                .userInfo(userInfo)
                .userContext(userContext)
                .build();
        final Authenticator.Result acceptance = Authenticator.Result.accept(user, () -> ImmutableMap.of());
        assertThat(acceptance.isAccepted(), is(true));
        assertThat(acceptance.getAuthenticatedUser().get().getSiteId(), is(user.getSiteId()));
        assertThat(acceptance.getAuthenticatedUser().get().isAdmin(), is(user.isAdmin()));
        assertThat(acceptance.getAuthenticatedUser().get().getUserInfo(), is(userInfo));
        assertThat(acceptance.getAuthenticatedUser().get().getUserContext(), is(userContext));
    }

    @Test
    public void admin()
            throws Exception
    {
        final AuthenticatedUser user = AuthenticatedUser.builder()
                .siteId(17)
                .isAdmin(true)
                .userInfo(CF.create())
                .userContext(CF.create())
                .build();
        final Authenticator.Result acceptance = Authenticator.Result.accept(user, () -> ImmutableMap.of());
        assertThat(acceptance.isAccepted(), is(true));
        assertThat(acceptance.getAuthenticatedUser().get().isAdmin(), is(true));
    }
}
