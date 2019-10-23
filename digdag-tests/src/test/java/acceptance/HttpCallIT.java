package acceptance;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jetty.http.HttpHeader.AUTHORIZATION;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.objectMapper;
import static utils.TestUtils.runWorkflow;
import static utils.TestUtils.startMockWebServer;
import utils.CommandStatus;
import utils.TestUtils;

public class HttpCallIT
{
    private static final ConfigFactory CF = TestUtils.configFactory();
    private static final YamlConfigLoader Y = new YamlConfigLoader();

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
        httpMockWebServer.setDispatcher(new QueueDispatcher());
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

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private Config loadYamlResource(String name)
    {
        try {
            String content = Resources.toString(Resources.getResource(name), UTF_8);
            return Y.loadString(content).toConfig(CF);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testSubTasksByJson()
            throws Exception
    {
        Config bodyConfig = loadYamlResource("acceptance/http_call/child.dig");
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/json")
                .setBody(bodyConfig.toString()));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(Files.exists(root().resolve("child.out")), is(true));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getMethod(), is("GET"));
    }

    @Test
    public void testSubTasksByYaml()
            throws Exception
    {
        Config bodyConfig = loadYamlResource("acceptance/http_call/child.dig");
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/x-yaml")
                .setBody(formatYaml(bodyConfig)));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(Files.exists(root().resolve("child.out")), is(true));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getMethod(), is("GET"));
    }

    @Test
    public void testSubTasksWithUnknownContentType()
            throws Exception
    {
        Config bodyConfig = loadYamlResource("acceptance/http_call/child.dig");
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/octet-stream")
                .setBody(formatYaml(bodyConfig)));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        CommandStatus status = runWorkflow(folder, "acceptance/http_call/http_call.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"), ImmutableMap.of(), 1);
        assertThat(status.errUtf8(), containsString("Unsupported Content-Type"));
        assertThat(status.errUtf8(), containsString("application/octet-stream"));
    }

    @Test
    public void testSubTasksWithContentTypeOverride()
            throws Exception
    {
        Config bodyConfig = loadYamlResource("acceptance/http_call/child.dig");
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/octet-stream")
                .setBody(formatYaml(bodyConfig)));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call_yaml_override.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(Files.exists(root().resolve("child.out")), is(true));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getMethod(), is("GET"));
    }

    private String formatYaml(Config value)
    {
        try {
            StringWriter writer = new StringWriter();
            try (YAMLGenerator out = new YAMLFactory().createGenerator(writer)) {
                objectMapper().writeValue(out, value);
            }
            return writer.toString();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testSubTasksByInvalidJson()
            throws Exception
    {
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/json")
                .setBody("!invalid!"));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"), ImmutableMap.of(), 1);
    }

    @Test
    public void testSubTasksByInvalidYaml()
            throws Exception
    {
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/x-yaml")
                .setBody("!invalid!"));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"), ImmutableMap.of(), 1);
    }

    @Test
    public void testSubTasksWithQueryString()
            throws Exception
    {
        Config bodyConfig = loadYamlResource("acceptance/http_call/child.dig");
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/json")
                .setBody(bodyConfig.toString()));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call_query.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(Files.exists(root().resolve("child.out")), is(true));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getMethod(), is("GET"));
        assertThat(request.getRequestUrl().queryParameter("k"), is("v"));
        assertThat(request.getRequestUrl().queryParameter("foo"), is("bar"));
    }

    @Test
    public void testSubTasksByPost()
            throws Exception
    {
        Config bodyConfig = loadYamlResource("acceptance/http_call/child.dig");
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/json")
                .setBody(bodyConfig.toString()));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call_post.dig", ImmutableMap.of(
                    "test_uri", uri,
                    "outdir", root().toString(),
                    "name", "child"));
        assertThat(httpMockWebServer.getRequestCount(), is(1));
        assertThat(Files.exists(root().resolve("child.out")), is(true));
        RecordedRequest request = httpMockWebServer.takeRequest();
        assertThat(request.getMethod(), is("POST"));
        assertThat(request.getBody().readUtf8(), is("{\"k\":\"v\",\"foo\":\"bar\"}"));
    }

    @Test
    public void testSystemProxy()
            throws Exception
    {
        proxy = TestUtils.startRequestFailingProxy(1, requests);
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/json")
                .setBody("{}"));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call.dig",
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
        httpMockWebServer.enqueue(
                new MockResponse()
                .addHeader("Content-Type: application/json")
                .setBody("{}"));
        String uri = "http://localhost:" + httpMockWebServer.getPort() + "/test";
        runWorkflow(folder, "acceptance/http_call/http_call.dig",
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
}
