package acceptance;

import com.amazonaws.util.Base64;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.netty.handler.codec.http.FullHttpRequest;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.TestUtils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jetty.http.HttpHeader.AUTHORIZATION;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.runWorkflow;
import static utils.TestUtils.startMockWebServer;

public class HttpIT
{
    private MockWebServer mockWebServer;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private HttpProxyServer proxy;
    private ConcurrentMap<String, List<FullHttpRequest>> requests;

    @Before
    public void setUp()
            throws Exception
    {
        mockWebServer = startMockWebServer();
        requests = new ConcurrentHashMap<>();
    }

    @After
    public void tearDownProxy()
            throws Exception
    {
        if (proxy != null) {
            proxy.stop();
        }
    }

    @After
    public void tearDownWebServer()
            throws Exception
    {
        if (mockWebServer != null) {
            mockWebServer.shutdown();
        }
    }

    @Test
    public void testSimple()
            throws Exception
    {
        String uri = "http://localhost:" + mockWebServer.getPort() + "/";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of("test_uri", uri));
        assertThat(mockWebServer.getRequestCount(), is(1));
    }

    @Test
    public void testSystemProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(3, requests);
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri
                ),
                ImmutableMap.of(
                        "config.http.proxy.enabled", "true",
                        "config.http.proxy.host", "localhost",
                        "config.http.proxy.port", Integer.toString(proxy.getListenAddress().getPort())
                ));
        assertThat(mockWebServer.getRequestCount(), is(1));
        assertThat(requests.get(uri), is(not(empty())));
    }

    @Test
    public void testUserProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(3, requests);
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.proxy.enabled", "true",
                        "http.proxy.host", "localhost",
                        "http.proxy.port", Integer.toString(proxy.getListenAddress().getPort())
                ));
        assertThat(mockWebServer.getRequestCount(), is(1));
        assertThat(requests.get("GET " + uri), is(not(nullValue())));
        assertThat(requests.get("GET " + uri), is(not(empty())));
    }

    @Test
    public void testDisableUserProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(3, requests);
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.proxy.enabled", "true",
                        "http.proxy.host", "localhost",
                        "http.proxy.port", Integer.toString(proxy.getListenAddress().getPort())
                ),
                ImmutableMap.of(
                        "config.http.allow_user_proxy", "false"
                ));
        assertThat(mockWebServer.getRequestCount(), is(1));
        assertThat(requests.entrySet(), is(empty()));
    }

    @Test
    public void testBasicAuth()
            throws Exception
    {
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of("test_uri", uri), ImmutableMap.of(
                "secrets.http.user", "test-user",
                "secrets.http.password", "test-pass"));
        assertThat(mockWebServer.getRequestCount(), is(1));
        RecordedRequest request = mockWebServer.takeRequest();

        assertThat(request.getHeader(AUTHORIZATION.asString()), is("Basic " + Base64.encodeAsString("test-user:test-pass".getBytes(UTF_8))));
    }

    @Test
    public void testCustomAuth()
            throws Exception
    {
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of("test_uri", uri), ImmutableMap.of(
                "secrets.http.authorization", "Bearer badf00d"));
        assertThat(mockWebServer.getRequestCount(), is(1));
        RecordedRequest request = mockWebServer.takeRequest();

        assertThat(request.getHeader(AUTHORIZATION.asString()), is("Bearer badf00d"));
    }

    @Test
    public void testPost()
            throws Exception
    {
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.method", "POST",
                        "http.content", "test-content",
                        "http.content_type", "text/plain"
                ));
        assertThat(mockWebServer.getRequestCount(), is(1));
        RecordedRequest request = mockWebServer.takeRequest();

        assertThat(request.getMethod(), is("POST"));
        assertThat(request.getBody().readUtf8(), is("test-content"));
        assertThat(request.getHeader(CONTENT_TYPE.asString()), is("text/plain"));
    }

    @Test
    public void testCustomHeaders()
            throws Exception
    {
        String uri = "http://localhost:" + mockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http_headers.dig",
                ImmutableMap.of(
                        "test_uri", uri
                ));
        assertThat(mockWebServer.getRequestCount(), is(1));
        RecordedRequest request = mockWebServer.takeRequest();

        Headers h = request.getHeaders();

        // Find first "foo" header
        int i = 0;
        for (; i < h.size() && !h.name(i).equals("foo"); i++) {
        }
        assertThat(i, is(lessThan(h.size())));

        // Verify header ordering
        assertThat(h.name(i), is("foo"));
        assertThat(h.value(i), is("foo-value-1"));

        assertThat(h.name(i + 1), is("bar"));
        assertThat(h.value(i + 1), is("bar-value"));

        assertThat(h.name(i + 2), is("foo"));
        assertThat(h.value(i + 2), is("foo-value-2"));
    }

    @Test
    public void testForEach()
            throws Exception
    {
        String uri = "http://localhost:" + mockWebServer.getPort() + "/";
        mockWebServer.setDispatcher(new QueueDispatcher());
        String content = DigdagClient.objectMapper().writeValueAsString(
                ImmutableList.of("foo", "bar", "baz"));
        mockWebServer.enqueue(new MockResponse().setBody(content));
        runWorkflow(folder, "acceptance/http/http_for_each.dig", ImmutableMap.of("test_uri", uri));
        assertThat(mockWebServer.getRequestCount(), is(1));
    }
}
