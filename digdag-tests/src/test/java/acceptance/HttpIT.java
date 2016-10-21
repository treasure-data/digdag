package acceptance;

import com.amazonaws.util.Base64;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.FullHttpRequest;
import okhttp3.mockwebserver.MockWebServer;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
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
        proxy = TestUtils.startRequestFailingProxy(3, requests);
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

        assertThat(request.getHeader(AUTHORIZATION.name()), is("Basic " + Base64.encodeAsString("test-user:test-pass".getBytes(UTF_8))));
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

        assertThat(request.getHeader(AUTHORIZATION.name()), is("Bearer badf00d"));
    }
}
