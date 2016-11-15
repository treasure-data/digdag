package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestApiKey;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import utils.TemporaryDigdagServer;

import javax.ws.rs.NotAuthorizedException;

import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static org.junit.Assert.assertThat;

public class AuthIT
{
    private static final RestApiKey apikey = RestApiKey.randomGenerate();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration("server.apikey = " + apikey)
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
    public void verifyOptionsRequestsDoNotNeedAuthentication()
            throws Exception
    {
        ContentResponse response = httpClient.newRequest(server.endpoint())
                .method("OPTIONS")
                .path("/api/workflows")
                .send();

        assertThat(response.getStatus(), Matchers.is(200));
    }

    @Test
    public void verifyTraceRequestsDoNotNeedAuthentication()
            throws Exception
    {
        ContentResponse response = httpClient.newRequest(server.endpoint())
                .method("TRACE")
                .path("/api/workflows")
                .send();

        assertThat(response.getStatus(), Matchers.is(200));
    }

    @Test
    public void verifyUnauthorizedRequestFails()
            throws Exception
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
            throws Exception
    {
        String token = Jwts.builder()
                .setHeaderParam("knd", "ps1")
                .setSubject(apikey.getIdString())
                .signWith(SignatureAlgorithm.HS256, apikey.getSecret())
                .compact();

        try (DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .header(AUTHORIZATION, "Bearer " + token)
                .build()) {

            digdagClient.getProjects();
        }
    }
}
