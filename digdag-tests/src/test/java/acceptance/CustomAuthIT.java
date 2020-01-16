package acceptance;

import io.digdag.client.DigdagClient;
import javax.ws.rs.NotAuthorizedException;
import org.eclipse.jetty.client.HttpClient;
import org.jboss.resteasy.util.BasicAuthHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.main;
import utils.TemporaryDigdagServer;

public class CustomAuthIT
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration("server.authenticator.type = testing")
            .configuration("server.authenticator.testing.header = X-Test")
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
    public void verifyAuthorizedRequestSucceeds()
    {
        try (DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .header("X-Test", "abc")
                .build()) {

            digdagClient.getProjects();
        }
    }

}
