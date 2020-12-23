package acceptance;

import com.amazonaws.util.Base64;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.netty.handler.codec.http.FullHttpRequest;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.jetty.http.HttpMethod;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.CommandStatus;
import utils.TestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.io.Resources.getResource;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jetty.http.HttpHeader.AUTHORIZATION;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.eclipse.jetty.http.HttpMethod.DELETE;
import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.HEAD;
import static org.eclipse.jetty.http.HttpMethod.OPTIONS;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.eclipse.jetty.http.HttpMethod.PUT;
import static org.eclipse.jetty.http.HttpMethod.TRACE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.runWorkflow;
import static utils.TestUtils.startMockWebServer;

public class HttpIT
{
    private static final HttpMethod[] METHODS = {GET, POST, HEAD, PUT, OPTIONS, DELETE, TRACE};
    private static final HttpMethod[] SAFE_METHODS = {GET, OPTIONS, HEAD, TRACE};
    private static final HttpMethod[] UNSAFE_METHODS = {POST, PUT, DELETE};

    private static final ObjectMapper OBJECT_MAPPER = DigdagClient.objectMapper();

    private MockWebServer httpMockWebServer;
    private MockWebServer httpsMockWebServer;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private HttpProxyServer proxy;
    private ConcurrentMap<String, List<FullHttpRequest>> requests;

    @Before
    public void setUp()
            throws Exception
    {
        httpMockWebServer = startMockWebServer();
        httpsMockWebServer = startMockWebServer(true);
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
    public void tearDownHttpServer()
            throws Exception
    {
        if (httpMockWebServer != null) {
            httpMockWebServer.shutdown();
        }
    }

    @After
    public void tearDownHttpsServer()
            throws Exception
    {
        if (httpsMockWebServer != null) {
            httpsMockWebServer.shutdown();
        }
    }

    @Test
    public void testHttp()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of("test_uri", uri));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
    }

    @Test
    public void testHttps()
            throws Exception
    {
        String uri = "https://localhost:" + httpsMockWebServer.getPort() + "/";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of(
                "test_uri", uri,
                "http.insecure", "true"));
        assertThat(httpsMockWebServer.getRequestCount(), is(1));
    }

    @Test
    public void testContentJsonObject()
            throws Exception
    {
        verifyJsonContent("object.json", testRequest("http_content_object.dig"));
    }

    @Test
    public void testContentJsonArray()
            throws Exception
    {
        verifyJsonContent("array.json", testRequest("http_content_array.dig"));
    }

    @Test
    public void testContentFormObject()
            throws Exception
    {
        verifyFormContent("flat_object.form", testRequest("http_content_flat_object.dig"));
    }

    @Test
    public void testContentScalar()
            throws Exception
    {
        RecordedRequest recordedRequest = testRequest("http_content_scalar.dig");
        assertThat(recordedRequest.getHeader("content-type"), is("plain/text"));
        assertThat(recordedRequest.getBody().readUtf8(), is("hello foo_secret_content_value_1_bar"));
    }

    private void verifyFormContent(String expectedContent, RecordedRequest request)
            throws IOException
    {
        assertThat(request.getHeader("content-type"), is(Matchers.equalToIgnoringCase("application/x-www-form-urlencoded")));
        String expectedString = Resources.toString(getResource("acceptance/http/" + expectedContent), UTF_8);
        assertThat(request.getBody().readUtf8(), is(expectedString));
    }

    private void verifyJsonContent(String expectedContent, RecordedRequest request)
            throws IOException
    {
        assertThat(request.getHeader("content-type"), is(Matchers.equalToIgnoringCase("application/json")));
        String body = request.getBody().readUtf8();
        JsonNode bodyJson = OBJECT_MAPPER.readTree(body);
        JsonNode expectedJson = OBJECT_MAPPER.readTree(getResource("acceptance/http/" + expectedContent));
        assertThat(bodyJson, is(expectedJson));
    }

    private RecordedRequest testRequest(String workflow)
            throws IOException, InterruptedException
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/";
        runWorkflow(folder, "acceptance/http/" + workflow, ImmutableMap.of("test_uri", uri), ImmutableMap.of(
                "secrets.content_value_1", "secret_content_value_1",
                "secrets.content_name_2", "secret_content_name_2",
                "secrets.content_value_2", "secret_content_value_2"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        return httpMockWebServer.takeRequest();
    }

    @Test
    public void testSystemProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(1, requests);
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri
                ),
                ImmutableMap.of(
                        "config.http.proxy.enabled", "true",
                        "config.http.proxy.host", "localhost",
                        "config.http.proxy.port", Integer.toString(proxy.getListenAddress().getPort())
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(requests.get(uri), is(not(empty())));
    }

    @Test
    public void testUserProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(1, requests);
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.proxy.enabled", "true",
                        "http.proxy.host", "localhost",
                        "http.proxy.port", Integer.toString(proxy.getListenAddress().getPort())
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(requests.get("GET " + uri), is(not(nullValue())));
        assertThat(requests.get("GET " + uri), is(not(empty())));
    }

    @Test
    public void testProxy403ForbiddenOnCONNECTIsNotRetried()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(100, requests, FORBIDDEN);
        String uri = "https://127.0.0.1:" + httpsMockWebServer.getPort() + "/";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.proxy.enabled", "true",
                        "http.proxy.host", "127.0.0.1",
                        "http.proxy.port", Integer.toString(proxy.getListenAddress().getPort())
                ),
                ImmutableMap.of(),
                1);
        assertThat(httpsMockWebServer.getRequestCount(), is(0));
        List<FullHttpRequest> proxyRequests = requests.get("CONNECT 127.0.0.1:" + httpsMockWebServer.getPort());
        assertThat(proxyRequests, is(not(nullValue())));
        assertThat(proxyRequests, hasSize(1));
    }

    @Test
    public void testDisableUserProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(3, requests);
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
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
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(requests.entrySet(), is(empty()));
    }

    @Test
    public void testBasicAuth()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of("test_uri", uri), ImmutableMap.of(
                "secrets.http.user", "test-user",
                "secrets.http.password", "test-pass"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        RecordedRequest request = httpMockWebServer.takeRequest();

        assertThat(request.getHeader(AUTHORIZATION.asString()), is("Basic " + Base64.encodeAsString("test-user:test-pass".getBytes(UTF_8))));
    }

    @Test
    public void testCustomAuth()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig", ImmutableMap.of("test_uri", uri), ImmutableMap.of(
                "secrets.http.authorization", "Bearer badf00d"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        RecordedRequest request = httpMockWebServer.takeRequest();

        assertThat(request.getHeader(AUTHORIZATION.asString()), is("Bearer badf00d"));
    }

    @Test
    public void testPost()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.method", "POST",
                        "http.content", "test-content",
                        "http.content_type", "text/plain"
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        RecordedRequest request = httpMockWebServer.takeRequest();

        assertThat(request.getMethod(), is("POST"));
        assertThat(request.getBody().readUtf8(), is("test-content"));
        assertThat(request.getHeader(CONTENT_TYPE.asString()), is("text/plain"));
    }

    @Test
    public void testQueryParameters()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.query", "{\"n1\":\"v1\",\"n2\":\"v &?2\"}"
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getPath(), is("/test?n1=v1&n2=v+%26%3F2"));
    }

    @Test
    public void testQueryString()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of(
                        "test_uri", uri,
                        "http.query", "n1=v1&n2=v+%26%3F2"
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getPath(), is("/test?n1=v1&n2=v+%26%3F2"));
    }

    @Test
    public void testEphemeralErrorsAreRetriedByDefaultForSafeMethods()
            throws Exception
    {
        verifyEphemeralErrorsAreRetried(SAFE_METHODS, ImmutableMap.of());
    }

    @Test
    public void testEphemeralErrorsAreNotRetriedByDefaultForUnsafeMethods()
            throws Exception
    {
        verifyEphemeralErrorsAreNotRetried(UNSAFE_METHODS, ImmutableMap.of());
    }

    @Test
    public void verifyEphemeralErrorsAreNotRetriedIfRetryIsDisabled()
            throws Exception
    {
        verifyEphemeralErrorsAreNotRetried(METHODS, ImmutableMap.of("http.retry", "false"));
    }

    @Test
    public void verifyEphemeralErrorsAreRetriedIfRetryIsEnabled()
            throws Exception
    {
        verifyEphemeralErrorsAreRetried(METHODS, ImmutableMap.of("http.retry", "true"));
    }

    private void verifyEphemeralErrorsAreNotRetried(HttpMethod[] methods, Map<String, String> params)
            throws IOException
    {
        proxy = TestUtils.startRequestFailingProxy(3, requests);
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        for (HttpMethod method : methods) {
            runWorkflow(folder, "acceptance/http/http.dig",
                    ImmutableMap.<String, String>builder()
                            .putAll(params)
                            .put("test_uri", uri)
                            .put("http.method", method.asString())
                            .put("http.proxy.enabled", "true")
                            .put("http.proxy.host", "localhost")
                            .put("http.proxy.port", Integer.toString(proxy.getListenAddress().getPort()))
                            .build(),
                    ImmutableMap.of(),
                    1);
            assertThat(requests.keySet().stream().anyMatch(k -> k.startsWith(method.asString())), is(true));
        }
        assertThat(httpMockWebServer.getRequestCount(), is(0));
    }

    private void verifyEphemeralErrorsAreRetried(HttpMethod[] methods, Map<String, String> params)
            throws IOException
    {
        proxy = TestUtils.startRequestFailingProxy(3, requests);
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        for (HttpMethod method : methods) {
            runWorkflow(folder, "acceptance/http/http.dig",
                    ImmutableMap.<String, String>builder()
                            .putAll(params)
                            .put("test_uri", uri)
                            .put("http.method", method.asString())
                            .put("http.proxy.enabled", "true")
                            .put("http.proxy.host", "localhost")
                            .put("http.proxy.port", Integer.toString(proxy.getListenAddress().getPort()))
                            .build(),
                    ImmutableMap.of(),
                    0);
            assertThat(requests.keySet().stream().anyMatch(k -> k.startsWith(method.asString())), is(true));
        }
        assertThat(requests.size(), is(methods.length));
        assertThat(httpMockWebServer.getRequestCount(), is(methods.length));
    }

    @Test
    public void testCustomHeaders()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http_headers.dig",
                ImmutableMap.of(
                        "test_uri", uri
                ),
                ImmutableMap.of(
                        "secrets.header_value_1", "secret_header_value_1",
                        "secrets.header_name_2", "secret_header_name_2",
                        "secrets.header_value_2", "secret_header_value_2"
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        RecordedRequest request = httpMockWebServer.takeRequest();

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

        assertThat(h.name(i + 3), is("plain_header_name_1"));
        assertThat(h.value(i + 3), is("secret_header_value_1"));

        assertThat(h.name(i + 4), is("secret_header_name_2"));
        assertThat(h.value(i + 4), is("secret_header_value_2"));
    }

    @Test
    public void testQuery()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http/http_query.dig",
                ImmutableMap.of(
                        "test_uri", uri
                ),
                ImmutableMap.of(
                        "secrets.arg_value_1", "secret_arg_value_1",
                        "secrets.arg_name_2", "secret_arg_name_2",
                        "secrets.arg_value_2", "secret_arg_value_2"
                ));
        assertThat(httpMockWebServer.getRequestCount(), is(3));

        for (int i = 0; i < 3; i++) {
            RecordedRequest request = httpMockWebServer.takeRequest();
            String query = Splitter.on('?').splitToList(request.getPath()).get(1);
            List<NameValuePair> queryArgs = URLEncodedUtils.parse(query, UTF_8);
            assertThat(queryArgs.get(0).getName(), is("foo"));
            assertThat(queryArgs.get(0).getValue(), is("bar"));
            assertThat(queryArgs.get(1).getName(), is("plain_arg_name_1"));
            assertThat(queryArgs.get(1).getValue(), is("secret_arg_value_1"));
            assertThat(queryArgs.get(2).getName(), is("secret_arg_name_2"));
            assertThat(queryArgs.get(2).getValue(), is("secret_arg_value_2"));
            System.out.println(queryArgs);
        }
    }

    @Test
    public void testForEach()
            throws Exception
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/";
        httpMockWebServer.setDispatcher(new QueueDispatcher());
        String content = DigdagClient.objectMapper().writeValueAsString(
                ImmutableList.of("foo", "bar", "baz"));
        httpMockWebServer.enqueue(new MockResponse().setBody(content));
        runWorkflow(folder, "acceptance/http/http_for_each.dig", ImmutableMap.of("test_uri", uri));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
    }

    @Test
    public void verifyErrorMessageSummary()
            throws IOException
    {
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        httpMockWebServer.setDispatcher(new QueueDispatcher());
        httpMockWebServer.enqueue(new MockResponse().setResponseCode(400).setBody("Test Failure Body"));
        CommandStatus status = runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of("test_uri", uri),
                ImmutableMap.of(),
                1);
        assertThat(status.errUtf8(), containsString("Test Failure Body"));
    }

    @Test
    public void verifyTruncateLongErrorMessage()
            throws IOException
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 197; i++) {
            sb.append(Integer.toString(i % 10));
        }
        String error197 = sb.toString();
        String fullError = error197 + "ERROR!";
        String truncatedError = error197 + "...";

        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        httpMockWebServer.setDispatcher(new QueueDispatcher());
        httpMockWebServer.enqueue(new MockResponse().setResponseCode(400).setBody(fullError));
        CommandStatus status = runWorkflow(folder, "acceptance/http/http.dig",
                ImmutableMap.of("test_uri", uri),
                ImmutableMap.of(),
                1);
        assertThat(status.errUtf8(), containsString(truncatedError));
    }
}
