package acceptance;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.client.entity.DeflateInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import utils.TemporaryDigdagServer;

public class HttpContentEncodingIT
{
    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private OkHttpClient client;

    @Before
    public void setup()
    {
        client = new OkHttpClient();
    }

    @Test
    public void testResponseContentEncoding()
            throws Exception
    {
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/version")
                .header("Accept-Encoding", "deflate")
                .build())
                .execute();
        assertThat(response.code(), is(200));
        assertThat(response.header("Content-Encoding"), is("deflate"));
        try (DeflateInputStream in = new DeflateInputStream(response.body().byteStream())) {
            String body = new String(ByteStreams.toByteArray(in), UTF_8);
            assertThat(body.startsWith("{"), is(true));
        }
    }

    @Test
    public void testResponseContentEncodingPriority()
            throws Exception
    {
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/version")
                .header("Accept-Encoding", "deflate,gzip")
                .build())
                .execute();
        assertThat(response.code(), is(200));
        assertThat(response.header("Content-Encoding"), is("gzip"));  // gzip has higher priority
        try (GZIPInputStream in = new GZIPInputStream(response.body().byteStream())) {
            String body = new String(ByteStreams.toByteArray(in), UTF_8);
            assertThat(body.startsWith("{"), is(true));
        }
    }

    @Test
    public void testRequestContentEncoding()
            throws Exception
    {
        String dummyRequest = "{\"fromTime\":\"1987-01-01T00:00:00+00:00\",\"dryRun\":true,\"attemptName\":\"\"}";
        byte[] compressedRequest;
        {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            try (GZIPOutputStream gz = new GZIPOutputStream(bo)) {
                gz.write(dummyRequest.getBytes(UTF_8));
            }
            compressedRequest = bo.toByteArray();
        }
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/schedules/99999/backfill")
                .header("Content-Encoding", "gzip")
                .post(RequestBody.create(MediaType.parse("application/json"), compressedRequest))
                .build())
                .execute();
        assertThat(response.code(), is(404));  // If Content-Encoding is not handled, this should be other code
        // because returning 404 means that the server succeeded to decode RestScheduleBackfillRequest object.
        assertThat(response.header("Content-Encoding"), nullValue());
    }
}
