package acceptance;

import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.List;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import utils.TemporaryDigdagServer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class ServerErrorStatusCodeIT
{
    private List<String> READ_METHODS = Arrays.asList(
            "GET", "OPTIONS", "HEAD"
    );

    private List<String> WRITE_METHODS = Arrays.asList(
            "POST", "PUT", "DELETE"
    );

    private List<String> UNKNOWN_METHODS = Arrays.asList(
            "LOCK", "INVALID_METHOD_NAME"
    );

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private OkHttpClient client;

    @Before
    public void setUp()
    {
        client = new OkHttpClient();
    }

    @Test
    public void verify200OkOnTrace()
            throws Exception
    {
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/no_such_api")
                .method("TRACE", null)
                .build()).execute();
        assertThat(response.code(), is(200));
    }

    @Test
    public void verify404NotFoundOnUnmatchingApplicationResource()
            throws Exception
    {
        for (String httpMethod : Iterables.concat(READ_METHODS, WRITE_METHODS, UNKNOWN_METHODS)) {
            Response response = client.newCall(new Request.Builder()
                    .url(server.endpoint() + "/api/no_such_api")
                    .method(httpMethod,
                        WRITE_METHODS.contains(httpMethod) ?
                            RequestBody.create(MediaType.parse("text/plain"), "") : null)
                    .build()).execute();
            assertThat(httpMethod, response.code(), is(404));
        }
    }

    @Test
    public void verify405MethodNotAllowedOnUiServlet()
            throws Exception
    {
        for (String httpMethod : Iterables.concat(WRITE_METHODS, UNKNOWN_METHODS)) {
            Response response = client.newCall(new Request.Builder()
                    .url(server.endpoint() + "/ui")
                    .method(httpMethod,
                        WRITE_METHODS.contains(httpMethod) ?
                            RequestBody.create(MediaType.parse("text/plain"), "") : null)
                    .build()).execute();
            assertThat(httpMethod, response.code(), is(405));
        }
    }

    @Test
    public void verify405MethodNotAllowedOnUnknownMethods()
            throws Exception
    {
        for (String httpMethod : UNKNOWN_METHODS) {
            Response response = client.newCall(new Request.Builder()
                    .url(server.endpoint() + "/api/version")
                    .method(httpMethod, null)
                    .build()).execute();
            assertThat(httpMethod, response.code(), is(405));
        }
    }

    @Test
    public void verify405MethodNotAllowedOnUnsupportedMethods()
            throws Exception
    {
        for (String httpMethod : WRITE_METHODS) {
            Response response = client.newCall(new Request.Builder()
                    .url(server.endpoint() + "/api/version")
                    .method(httpMethod, RequestBody.create(MediaType.parse("text/plain"), ""))
                    .build()).execute();
            assertThat(httpMethod, response.code(), is(405));
        }
    }

    @Test
    public void verify404NotFoundOnMalformedId()
            throws Exception
    {
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/projects/no_such_id")
                .build()).execute();
        assertThat(response.code(), is(404));
    }
}
