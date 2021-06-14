package acceptance;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import utils.TemporaryDigdagServer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UiResourceIT
{
    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration()
            .build();

    private Client http;

    @Before
    public void setUp()
            throws Exception
    {
        http = new ResteasyClientBuilder()
            .build();
    }

    @Test
    public void serveRoot()
            throws Exception
    {
        String root = target("/")
            .request("text/html")
            .get(String.class);
        assertThat(root.trim(), is("INDEX"));
    }

    @Test
    public void serveTopLevelFiles()
            throws Exception
    {
        String index = target("/index.html")
            .request("text/html")
            .get(String.class);
        assertThat(index.trim(), is("INDEX"));

        String png = target("/IT.png")
            .request("image/png")
            .get(String.class);
        assertThat(png.trim(), is("PNG"));
    }

    @Test
    public void serveAppRedirects()
            throws Exception
    {
        String app = target("/projects/20")
            .request("text/html")
            .get(String.class);
        assertThat(app.trim(), is("INDEX"));
    }

    @Test
    public void serveAssets()
            throws Exception
    {
        String css = target("/assets/it.css")
            .request("text/css")
            .get(String.class);
        assertThat(css.trim(), is("CSS"));

        Response response = target("/assets/no_such_asset.css")
            .request()
            .get();
        assertThat(response.getStatus(), is(404));
    }

    private WebTarget target(String path)
    {
        return http.target(UriBuilder.fromUri("http://" + server.host() + ":" + server.port() + path));
    }
}
