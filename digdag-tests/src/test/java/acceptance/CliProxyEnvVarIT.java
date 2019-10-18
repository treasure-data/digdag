package acceptance;

import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpRequest;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.tls.HandshakeCertificates;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.littleshoot.proxy.ActivityTrackerAdapter;
import org.littleshoot.proxy.FlowContext;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.extras.SelfSignedSslEngineSource;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandStatus;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.digdag.client.DigdagVersion.buildVersion;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static okhttp3.tls.internal.TlsUtil.localhost;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.main;

public class CliProxyEnvVarIT
{
    private static final Logger logger = LoggerFactory.getLogger(CliProxyEnvVarIT.class);

    private static final MockResponse VERSION_RESPONSE = new MockResponse()
            .setBody("{\"version\":\"" + buildVersion() + "\"}")
            .setHeader(CONTENT_TYPE, "application/json");

    private HttpProxyServer httpProxy;
    private HttpProxyServer httpsProxy;
    private String httpProxyUrl;
    private String httpsProxyUrl;

    private MockWebServer httpMockServer;
    private MockWebServer httpsMockServer;

    private final HttpProxyRequestTracker httpProxyRequestTracker = new HttpProxyRequestTracker();
    private final HttpProxyRequestTracker httpsProxyRequestTracker = new HttpProxyRequestTracker();

    @Before
    public void setUp()
            throws Exception
    {
        httpMockServer = new MockWebServer();
        httpMockServer.start();

        httpsMockServer = new MockWebServer();
        HandshakeCertificates handshakeCertificates = localhost();
        SSLSocketFactory socketFactory = handshakeCertificates.sslSocketFactory();


        httpsMockServer.useHttps(socketFactory, false);
        httpsMockServer.start();

        httpProxy = DefaultHttpProxyServer
                .bootstrap()
                .withPort(0)
                .plusActivityTracker(httpProxyRequestTracker)
                .start();
        httpProxyUrl = "http://" + httpProxy.getListenAddress().getHostString() + ":" + httpProxy.getListenAddress().getPort();

        httpsProxy = DefaultHttpProxyServer
                .bootstrap()
                .withPort(0)
                .plusActivityTracker(httpsProxyRequestTracker)
                .withSslEngineSource(new SelfSignedSslEngineSource())
                .withAuthenticateSslClients(false)
                .start();
        httpsProxyUrl = "https://" + httpsProxy.getListenAddress().getHostString() + ":" + httpsProxy.getListenAddress().getPort();
    }

    @Test
    public void testCliProxying__HTTP__over_HTTP___http_proxy()
    {
        assertCliUsesProxy(ImmutableMap.of("http_proxy", httpProxyUrl), httpMockServer, httpProxyRequestTracker);
    }

    @Test
    public void testCliProxying__HTTP__over_HTTP___HTTP_PROXY()
    {
        assertCliUsesProxy(ImmutableMap.of("HTTP_PROXY", httpProxyUrl), httpMockServer, httpProxyRequestTracker);
    }

    @Ignore("HTTP over HTTPS not supported by apache http client")
    @Test
    public void testCliProxying__HTTP__over_HTTPS__http_proxy()
    {
        assertCliUsesProxy(ImmutableMap.of("http_proxy", httpsProxyUrl), httpMockServer, httpsProxyRequestTracker);
    }

    @Ignore("HTTP over HTTPS not supported by apache http client")
    @Test
    public void testCliProxying__HTTP__over_HTTPS__HTTP_PROXY()
    {
        assertCliUsesProxy(ImmutableMap.of("HTTP_PROXY", httpsProxyUrl), httpMockServer, httpsProxyRequestTracker);
    }

    @Test
    public void testCliProxying__HTTPS_over_HTTPS__http_proxy()
    {
        assertCliUsesProxy(ImmutableMap.of("http_proxy", httpProxyUrl), httpsMockServer, httpProxyRequestTracker);
    }

    @Test
    public void testCliProxying__HTTPS_over_HTTP___HTTP_PROXY()
    {
        assertCliUsesProxy(ImmutableMap.of("HTTP_PROXY", httpProxyUrl), httpsMockServer, httpProxyRequestTracker);
    }

    @Test
    public void testCliProxying__HTTPS_over_HTTPS__https_proxy()
    {
        assertCliUsesProxy(ImmutableMap.of("https_proxy", httpsProxyUrl), httpsMockServer, httpsProxyRequestTracker);
    }

    @Test
    public void testCliProxying__HTTPS_over_HTTPS__HTTPS_PROXY()
    {
        assertCliUsesProxy(ImmutableMap.of("HTTPS_PROXY", httpsProxyUrl), httpsMockServer, httpsProxyRequestTracker);
    }

    private void assertCliUsesProxy(Map<String, String> env, MockWebServer server, HttpProxyRequestTracker proxyRequestTracker)
    {
        String endpoint = server.url("/").toString();
        logger.info("server endpoint: {}, env: {}", endpoint, env);
        server.enqueue(VERSION_RESPONSE);
        CommandStatus versionStatus = main(env, "version", "-c", "/dev/null", "-e", endpoint, "--disable-cert-validation");
        assertThat(versionStatus.errUtf8(), versionStatus.code(), is(0));
        assertThat(server.getRequestCount(), is(1));
        assertThat(proxyRequestTracker.clientRequestsReceived.get(), is(1));
        assertThat(versionStatus.outUtf8(), Matchers.containsString("Server version: " + buildVersion()));
    }

    private class HttpProxyRequestTracker
            extends ActivityTrackerAdapter
    {
        private final AtomicInteger clientRequestsReceived = new AtomicInteger();

        @Override
        public void requestReceivedFromClient(FlowContext flowContext, HttpRequest httpRequest)
        {
            clientRequestsReceived.incrementAndGet();
        }
    }
}
