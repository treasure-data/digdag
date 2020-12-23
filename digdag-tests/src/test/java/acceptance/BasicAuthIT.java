package acceptance;

import io.digdag.client.DigdagClient;
import org.eclipse.jetty.client.HttpClient;
import org.hamcrest.MatcherAssert;
import org.jboss.resteasy.util.BasicAuthHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import javax.ws.rs.NotAuthorizedException;

import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.main;

public class BasicAuthIT
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration("server.authenticator.type = basic")
            .configuration("server.authenticator.basic.username = user123")
            .configuration("server.authenticator.basic.password = secret456")
            .build();

    private HttpClient httpClient;

    @Before
    public void setUp()
            throws Exception
    {
        httpClient = new HttpClient();
        httpClient.start();
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (httpClient != null) {
            httpClient.stop();
        }
    }

    @Test
    public void verifyUnauthorizedRequestFails()
    {
        try (DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build()) {

            expectedException.expect(NotAuthorizedException.class);

            digdagClient.getProjects();
        }
    }

    @Test
    public void verifyUnauthorizedRequestFailsWithBadUsername()
    {
        try (DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .header(AUTHORIZATION, BasicAuthHelper.createHeader("bad username", "secret456"))
                .build()) {

            expectedException.expect(NotAuthorizedException.class);

            digdagClient.getProjects();
        }
    }

    @Test
    public void verifyUnauthorizedRequestFailsWithBadPassword()
    {
        try (DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .header(AUTHORIZATION, BasicAuthHelper.createHeader("user123", "bad password"))
                .build()) {

            expectedException.expect(NotAuthorizedException.class);

            digdagClient.getProjects();
        }
    }

    @Test
    public void verifyAuthorizedRequestSucceeds()
    {
        try (DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .header(AUTHORIZATION, BasicAuthHelper.createHeader("user123", "secret456"))
                .build()) {

            digdagClient.getProjects();
        }
    }

    @Test
    public void verifyAuthorizedRequestSucceedsUsingCli()
            throws Exception
    {

        CommandStatus showProjectStatus = main("projects",
                "-e", server.endpoint(),
                "--basic-auth", "user123:secret456"
        );

        MatcherAssert.assertThat(showProjectStatus.errUtf8(), showProjectStatus.code(), is(0));
    }
}
