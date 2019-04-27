package io.digdag.server;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.AuthenticatedUser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AuthRequestFilterTest
{
    private static final ConfigFactory CONFIG_FACTORY = new ConfigFactory(DigdagClient.objectMapper());

    @Mock ContainerRequestContext containerRequestContext;
    @Mock Authenticator authenticator;
    @Mock UriInfo uriInfo;

    private AuthRequestFilter authRequestFilter;

    @Before
    public void setUp()
            throws Exception
    {
        authRequestFilter = new AuthRequestFilter(authenticator, CONFIG_FACTORY);
    }

    @Test
    public void verifyThatRequestsAreAuthenticated()
            throws Exception
    {
        verifyThatMethodIsAuthenticated("HEAD");
        verifyThatMethodIsAuthenticated("GET");
        verifyThatMethodIsAuthenticated("PUT");
        verifyThatMethodIsAuthenticated("POST");
        verifyThatMethodIsAuthenticated("DELETE");
        verifyThatMethodIsAuthenticated("BAZ");
    }

    @Test
    public void verifyThatOptionsRequestsArePassed()
            throws Exception
    {
        verifyThatMethodIsPassed("OPTIONS");
    }

    @Test
    public void verifyThatTraceRequestsArePassed()
            throws Exception
    {
        verifyThatMethodIsPassed("TRACE");
    }

    @Test
    public void verifyThatVersionEndpointIsPassed()
            throws Exception
    {
        when(uriInfo.getPath()).thenReturn("/api/version");
        when(containerRequestContext.getUriInfo()).thenReturn(uriInfo);
        when(containerRequestContext.getMethod()).thenReturn("GET");
        authRequestFilter.filter(containerRequestContext);
        verify(containerRequestContext, never()).abortWith(any(Response.class));
        verify(authenticator, never()).authenticate(any(ContainerRequestContext.class));
    }

    private void verifyThatMethodIsPassed(String method)
    {
        when(containerRequestContext.getMethod()).thenReturn(method);
        authRequestFilter.filter(containerRequestContext);
        verify(containerRequestContext, never()).abortWith(any(Response.class));
        verify(authenticator, never()).authenticate(any(ContainerRequestContext.class));
    }

    private void verifyThatMethodIsAuthenticated(String method)
    {
        reset(authenticator);
        reset(containerRequestContext);
        reset(uriInfo);

        when(uriInfo.getPath()).thenReturn("/api/foobar");

        // Reject
        when(containerRequestContext.getUriInfo()).thenReturn(uriInfo);
        when(authenticator.authenticate(containerRequestContext)).thenReturn(Authenticator.Result.reject("reject"));
        when(containerRequestContext.getMethod()).thenReturn(method);
        authRequestFilter.filter(containerRequestContext);
        verify(containerRequestContext).getMethod();
        verify(authenticator).authenticate(containerRequestContext);
        verify(containerRequestContext).abortWith(any(Response.class));

        reset(authenticator);
        reset(containerRequestContext);

        // Accept
        final Config userInfo = CONFIG_FACTORY.create();
        final Config userContext = CONFIG_FACTORY.create();
        final Supplier<Map<String, String>> secrets = () -> ImmutableMap.of("secret", "value");
        final AuthenticatedUser user = AuthenticatedUser.builder()
                .siteId(17)
                .isAdmin(true)
                .userInfo(userInfo)
                .userContext(userContext)
                .build();
        final Authenticator.Result acceptance = Authenticator.Result.accept(user, secrets);

        when(containerRequestContext.getUriInfo()).thenReturn(uriInfo);
        when(authenticator.authenticate(containerRequestContext)).thenReturn(acceptance);
        when(containerRequestContext.getMethod()).thenReturn(method);
        authRequestFilter.filter(containerRequestContext);
        verify(containerRequestContext).getMethod();
        verify(authenticator).authenticate(containerRequestContext);
        verify(containerRequestContext, never()).abortWith(any(Response.class));
        verify(containerRequestContext).setProperty("authenticatedUser", user);
        verify(containerRequestContext).setProperty("secrets", secrets);
    }
}