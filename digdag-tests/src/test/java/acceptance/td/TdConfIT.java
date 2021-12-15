package acceptance.td;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static acceptance.td.Secrets.TD_API_KEY;
import static io.netty.handler.codec.http.HttpHeaders.Names.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.main;

public class TdConfIT
{
    private static final String MOCK_TD_API_KEY = "4711/badf00d";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path tdConf;

    private HttpProxyServer proxyServer;
    private List<FullHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());
    private Path digdagConf;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(TD_API_KEY, not(isEmptyOrNullString()));
        tdConf = folder.newFolder().toPath().resolve("td.conf");
        digdagConf = folder.newFolder().toPath().resolve("digdag");
        Files.write(digdagConf, emptyList());
    }

    @Before
    public void setupProxy()
    {
        proxyServer = DefaultHttpProxyServer
                .bootstrap()
                .withPort(0)
                .withFiltersSource(new HttpFiltersSourceAdapter()
                {
                    @Override
                    public int getMaximumRequestBufferSizeInBytes()
                    {
                        return 1024 * 1024;
                    }

                    @Override
                    public HttpFilters filterRequest(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext)
                    {
                        return new HttpFiltersAdapter(httpRequest)
                        {
                            @Override
                            public HttpResponse clientToProxyRequest(HttpObject httpObject)
                            {
                                assert httpObject instanceof FullHttpRequest;
                                FullHttpRequest req = ((FullHttpRequest) httpObject).copy();
                                requests.add(req);

                                if (req.getMethod() == HttpMethod.GET &&
                                        req.getUri().equals("http://foo.baz.bar:80/api/projects")) {
                                    return new DefaultFullHttpResponse(req.getProtocolVersion(), OK,
                                            Unpooled.wrappedBuffer("[]".getBytes(UTF_8)));
                                }
                                return null;
                            }
                        };
                    }
                }).start();
    }

    @After
    public void stopProxy()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
    }

    @Before
    public void enableClientConfiguration()
            throws Exception
    {
        System.setProperty("io.digdag.standards.td.client-configurator.enabled", "true");
        System.setProperty("io.digdag.standards.td.client-configurator.endpoint", "http://foo.baz.bar");
    }

    @After
    public void disableClientConfiguration()
            throws Exception
    {
        System.clearProperty("io.digdag.standards.td.client-configurator.enabled");
        System.clearProperty("io.digdag.standards.td.client-configurator.endpoint");
    }

    @Test
    public void verifyCliConfiguresDigdagClientUsingTdConf()
            throws Exception
    {
        Files.write(tdConf, asList(
                "[account]",
                "  user = foo@bar.com",
                "  apikey = " + MOCK_TD_API_KEY
        ));

        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();
        Map<String, String> env = ImmutableMap.of(
                "http_proxy", proxyUrl,
                "TD_CONFIG_PATH", tdConf.toString()
        );

        main(env, "workflows",
                "-c", digdagConf.toString(),
                "--disable-version-check");

        assertThat(requests.isEmpty(), is(false));
        for (FullHttpRequest request : requests) {
            assertThat(request.getUri(), is("http://foo.baz.bar:80/api/workflows?count=100"));
            assertThat(request.headers().get(AUTHORIZATION), is("TD1 " + MOCK_TD_API_KEY));
        }
    }

    @Test
    public void verifyCliIgnoresMissingTdConf()
            throws Exception
    {
        assertThat(Files.notExists(tdConf), is(true));

        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();
        Map<String, String> env = ImmutableMap.of(
                "http_proxy", proxyUrl,
                "TD_CONFIG_PATH", tdConf.toString()
        );

        Files.write(digdagConf, asList(
                "client.http.endpoint = http://baz.quux:80",
                "client.http.headers.authorization = FOO " + MOCK_TD_API_KEY
        ));

        main(env, "workflows",
                "-c", digdagConf.toString(),
                "--disable-version-check");

        assertThat(requests.isEmpty(), is(false));
        for (FullHttpRequest request : requests) {
            assertThat(request.getUri(), is("http://baz.quux:80/api/workflows?count=100"));
            assertThat(request.headers().get(AUTHORIZATION), is("FOO " + MOCK_TD_API_KEY));
        }
    }

    @Test
    public void verifyCliIgnoresBrokenTdConf()
            throws Exception
    {
        Files.write(tdConf, asList("endpoint=foo:bar"));

        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();
        Map<String, String> env = ImmutableMap.of(
                "http_proxy", proxyUrl,
                "TD_CONFIG_PATH", tdConf.toString()
        );

        Files.write(digdagConf, asList(
                "client.http.endpoint = http://baz.quux:80",
                "client.http.headers.authorization = FOO " + MOCK_TD_API_KEY
        ));

        main(env, "workflows",
                "-c", digdagConf.toString(),
                "--disable-version-check");

        assertThat(requests.isEmpty(), is(false));
        for (FullHttpRequest request : requests) {
            assertThat(request.getUri(), is("http://baz.quux:80/api/workflows?count=100"));
            assertThat(request.headers().get(AUTHORIZATION), is("FOO " + MOCK_TD_API_KEY));
        }
    }
}
