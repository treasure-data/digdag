package acceptance;

import com.treasuredata.client.TDClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import io.netty.util.ReferenceCountUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class TdIT
{
    private static final Logger logger = LoggerFactory.getLogger(TdIT.class);

    private static final String TD_API_KEY = System.getenv("TD_API_KEY");

    private static final String DOMAIN_KEY = "domain_key";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;
    private String database;

    private Path outfile;

    private HttpProxyServer proxyServer;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        Files.write(config, asList("params.td.apikey = " + TD_API_KEY));
        outfile = projectDir.resolve("outfile");

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);
    }

    @After
    public void deleteDatabase()
            throws Exception
    {
        if (client != null && database != null) {
            client.deleteDatabase(database);
        }
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

    @Test
    public void testRunQuery()
            throws Exception
    {
        copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        runWorkflow();
    }

    @Test
    public void testRunQueryInline()
            throws Exception
    {
        copyResource("acceptance/td/td/td_inline.dig", projectDir.resolve("workflow.dig"));
        runWorkflow();
    }

    @Test
    public void testRetries()
            throws Exception
    {
        int failures = 3;

        List<FullHttpRequest> jobIssueRequests = Collections.synchronizedList(new ArrayList<>());

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
                                FullHttpRequest fullHttpRequest = (FullHttpRequest) httpObject;
                                if (httpRequest.getUri().contains("/v3/job/issue")) {
                                    jobIssueRequests.add(fullHttpRequest.copy());
                                    if (jobIssueRequests.size() < failures) {
                                        logger.info("Simulating 500 INTERNAL SERVER ERROR for request: {}", httpRequest);
                                        DefaultFullHttpResponse response = new DefaultFullHttpResponse(httpRequest.getProtocolVersion(), INTERNAL_SERVER_ERROR);
                                        response.headers().set(CONNECTION, CLOSE);
                                        return response;
                                    }
                                }
                                logger.info("Passing request: {}", httpRequest);
                                return null;
                            }
                        };
                    }
                }).start();

        Files.write(config, asList(
                "params.td.use_ssl = false",
                "params.td.proxy.enabled = true",
                "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
        ), APPEND);

        copyResource("acceptance/td/td/td_inline.dig", projectDir.resolve("workflow.dig"));
        runWorkflow();

        for (FullHttpRequest request : jobIssueRequests) {
            ReferenceCountUtil.releaseLater(request);
        }

        assertThat(jobIssueRequests.size(), is(not(0)));

        // Verify that all job issue requests reuse the same domain key
        verifyDomainKeys(jobIssueRequests);
    }

    @Test
    public void testDomainKeyConflict()
            throws Exception
    {
        List<FullHttpRequest> jobIssueRequests = Collections.synchronizedList(new ArrayList<>());
        List<FullHttpResponse> jobIssueResponses = Collections.synchronizedList(new ArrayList<>());

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
                    public int getMaximumResponseBufferSizeInBytes()
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
                                FullHttpRequest fullHttpRequest = (FullHttpRequest) httpObject;
                                if (httpRequest.getUri().contains("/v3/job/issue")) {
                                    jobIssueRequests.add(fullHttpRequest.copy());
                                }
                                return null;
                            }

                            @Override
                            public HttpObject serverToProxyResponse(HttpObject httpObject)
                            {
                                assert httpObject instanceof FullHttpResponse;
                                FullHttpResponse fullHttpResponse = (FullHttpResponse) httpObject;

                                // Let the first issue request through so that the job is started and the domain key is recorded by the td api but
                                // simulate a failure for the first request. The td operator will then retry starting the job with the same domain key
                                // which will result in a conflict response. The td operator should then interpret that as the job having successfully been issued.
                                if (httpRequest.getUri().contains("/v3/job/issue")) {
                                    jobIssueResponses.add(fullHttpResponse);
                                    if (jobIssueResponses.size() == 1) {
                                        logger.info("Simulating 500 INTERNAL SERVER ERROR for request: {}", httpRequest);
                                        DefaultFullHttpResponse response = new DefaultFullHttpResponse(httpRequest.getProtocolVersion(), INTERNAL_SERVER_ERROR);
                                        response.headers().set(CONNECTION, CLOSE);
                                        return response;
                                    }
                                }
                                logger.info("Passing request: {}", httpRequest);
                                return httpObject;
                            }
                        };
                    }
                }).start();

        Files.write(config, asList(
                "params.td.use_ssl = false",
                "params.td.proxy.enabled = true",
                "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
        ), APPEND);

        copyResource("acceptance/td/td/td_inline.dig", projectDir.resolve("workflow.dig"));
        runWorkflow();

        for (FullHttpRequest request : jobIssueRequests) {
            ReferenceCountUtil.releaseLater(request);
        }

        assertThat(jobIssueRequests.size(), is(not(0)));
        assertThat(jobIssueResponses.size(), is(not(0)));

        verifyDomainKeys(jobIssueRequests);
    }

    private void verifyDomainKeys(List<FullHttpRequest> jobIssueRequests)
            throws IOException
    {
        // Verify that all job issue requests reuse the same domain key
        FullHttpRequest firstRequest = jobIssueRequests.get(0);
        String domainKey = domainKey(firstRequest);

        for (int i = 0; i < jobIssueRequests.size(); i++) {
            FullHttpRequest request = jobIssueRequests.get(i);
            String requestDomainKey = domainKey(request);
            assertThat(requestDomainKey, is(domainKey));
        }
    }

    private String domainKey(FullHttpRequest request)
            throws IOException
    {
        FullHttpRequest copy = request.copy();
        ReferenceCountUtil.releaseLater(copy);
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(copy);
        List<InterfaceHttpData> keyDatas = decoder.getBodyHttpDatas(DOMAIN_KEY);
        assertThat(keyDatas, is(not(nullValue())));
        assertThat(keyDatas.size(), is(1));
        InterfaceHttpData domainKeyData = keyDatas.get(0);
        assertThat(domainKeyData.getHttpDataType(), is(HttpDataType.Attribute));
        return ((Attribute) domainKeyData).getValue();
    }

    private void runWorkflow()
    {
        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "outfile=" + outfile,
                "workflow.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        assertThat(Files.exists(outfile), is(true));
    }
}
