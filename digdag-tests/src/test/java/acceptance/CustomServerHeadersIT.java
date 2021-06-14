package acceptance;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Rule;
import org.junit.Test;
import utils.TemporaryDigdagServer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CustomServerHeadersIT
{
    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "server.http.headers.Foo-Bar-1 = Baz-1",
                    "server.http.headers.Foo-Bar-2 = Baz-2")
            .addArgs("-H", "Foo-Bar-3=Baz-3")
            .addArgs("--header", "Foo-Bar-4=Baz-4")
            .build();

    @Test
    public void verifyServerSetsCustomHeadersOnResponse()
            throws Exception
    {
        OkHttpClient client = new OkHttpClient();

        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/version")
                .build())
                .execute();

        assertThat(response.header("Foo-Bar-1"), is("Baz-1"));
        assertThat(response.header("Foo-Bar-2"), is("Baz-2"));
        assertThat(response.header("Foo-Bar-3"), is("Baz-3"));
        assertThat(response.header("Foo-Bar-4"), is("Baz-4"));
    }
}
