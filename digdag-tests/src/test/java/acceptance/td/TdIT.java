package acceptance.td;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDApiRequest;
import com.treasuredata.client.TDClient;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;
import org.hamcrest.Matchers;
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
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;
import utils.TestUtils.RecordableWorkflow.ApiCallRecord;
import utils.TestUtils.RecordableWorkflow.CommandStatusAndRecordedApiCalls;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static acceptance.td.Secrets.ENCRYPTION_KEY;
import static acceptance.td.Secrets.TD_API_ENDPOINT;
import static acceptance.td.Secrets.TD_API_KEY;
import static acceptance.td.Secrets.TD_SECRETS_ENABLED_PROP_KEY;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.RecordableWorkflow.mainWithRecordableRun;
import static utils.TestUtils.objectMapper;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.startRequestTrackingProxy;

public class TdIT
{
    private static final Logger logger = LoggerFactory.getLogger(TdIT.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;
    private String database;

    private Path outfile;

    private HttpProxyServer proxyServer;

    private TemporaryDigdagServer server;
    private String noTdConf;

    private Map<String, String> env;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(TD_API_KEY, not(isEmptyOrNullString()));
        noTdConf = folder.newFolder().toPath().resolve("non-existing-td.conf").toString();
        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        Files.write(config, asList("secrets.td.apikey = " + TD_API_KEY));
        outfile = projectDir.resolve("outfile");

        env = new HashMap<>();
        env.put("TD_CONFIG_PATH", noTdConf);

        client = TDClient.newBuilder(false)
                .setEndpoint(TD_API_ENDPOINT)
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

    @After
    public void stopServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void testRunQuery()
            throws Exception
    {
        copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        assertWorkflowRunsSuccessfully();
    }

    @Test
    public void testRunQueryWithHiveAndResourcePool()
            throws Exception
    {
        copyResource("acceptance/td/td/td_hive.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        assertWorkflowRunsSuccessfully();
    }

    private void assertOutputOfTdStoreLastResult()
            throws Exception
    {
        JsonNode result = objectMapper().readTree(outfile.toFile());
        assertThat(result.get("last_job_id").asInt(), is(not(0)));
        assertThat(result.get("last_job").get("id").asInt(), is(not(0)));
        assertThat(result.get("last_job").get("num_records").asInt(), is(1));
        assertThat(result.get("last_results").isObject(), is(true));
        assertThat(result.get("last_results").isEmpty(objectMapper().getSerializerProvider()), is(false));
        assertThat(result.get("last_results").get("a").asInt(), is(1));
        assertThat(result.get("last_results").get("b").asInt(), is(2));
    }

    @Test
    public void testStoreLastResult()
            throws Exception
    {
        copyResource("acceptance/td/td/td_store_last_result.dig", projectDir.resolve("workflow.dig"));
        assertWorkflowRunsSuccessfully();
        assertOutputOfTdStoreLastResult();
    }

    @Test
    public void testStoreLastResultWithEmptyQueryResult()
            throws Exception
    {
        copyResource("acceptance/td/td/td_store_last_result_empty.dig", projectDir.resolve("workflow.dig"));
        assertWorkflowRunsSuccessfully();
        JsonNode result = objectMapper().readTree(outfile.toFile());
        assertThat(result.get("last_job_id").asInt(), is(not(0)));
        assertThat(result.get("last_job").get("id").asInt(), is(not(0)));
        assertThat(result.get("last_job").get("num_records").asInt(), is(0));
        assertThat(result.get("last_results").isObject(), is(true));
        assertThat(result.get("last_results").isEmpty(objectMapper().getSerializerProvider()), is(true));
    }

    @Test
    public void testStoreLastResultTwice()
            throws Exception
    {
        copyResource("acceptance/td/td/td_store_last_result_twice.dig", projectDir.resolve("workflow.dig"));
        assertWorkflowRunsSuccessfully();
        JsonNode result = objectMapper().readTree(outfile.toFile());
        assertThat(result.get("last_job_id").asInt(), is(not(0)));
        assertThat(result.get("last_results").isObject(), is(true));
        assertThat(result.get("last_results").size(), is(2));
        assertThat(result.get("last_results").get("a").textValue(), is("A2"));
        assertThat(result.get("last_results").get("d").textValue(), is("D"));
    }

    @Test
    public void testRunQueryWithEnvProxy()
            throws Exception
    {
        List<FullHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

        proxyServer = startRequestTrackingProxy(requests);

        copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();
        env.put("http_proxy", proxyUrl);
        CommandStatusAndRecordedApiCalls result = assertWorkflowRunsSuccessfullyAndReturnApiCalls("td.use_ssl=true");
        assertThat(result.apiCallRecords.stream().filter(req -> req.request.getPath().contains("/v3/job/issue")).count(), is(greaterThan(0L)));
    }

    @Test
    public void testRunQueryAndPickUpApiKeyFromTdConf()
            throws Exception
    {
        // Write apikey to td.conf
        Path tdConf = folder.newFolder().toPath().resolve("td.conf");
        Files.write(tdConf, asList(
                "[account]",
                "  user = foo@bar.com",
                "  apikey = " + TD_API_KEY,
                "  usessl = true"
        ));

        // Remove apikey from digdag conf
        Files.write(config, emptyList());

        System.setProperty(TD_SECRETS_ENABLED_PROP_KEY, "true");
        env.put("TD_CONFIG_PATH", tdConf.toString());

        try {
            copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
            copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));

            List<ApiCallRecord> issueRequestCalls = assertWorkflowRunsSuccessfullyAndReturnApiCalls().apiCallRecords.stream()
                    .filter(req -> req.request.getPath().contains("/v3/job/issue"))
                    .collect(toList());
            assertThat(issueRequestCalls.size(), is(greaterThan(0)));
            for (ApiCallRecord requestCall : issueRequestCalls) {
                assertThat(requestCall.apikeyCache.get(), is(TD_API_KEY));
            }
            assertThat(issueRequestCalls.size(), is(greaterThan(0)));
        }
        finally {
            System.setProperty(TD_SECRETS_ENABLED_PROP_KEY, "false");
        }
    }

    @Test
    public void testRunQueryOnServerWithEnvProxy()
            throws Exception
    {
        List<FullHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

        proxyServer = startRequestTrackingProxy(requests);

        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();

        TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .configuration(Secrets.secretsServerConfiguration())
                .environment(ImmutableMap.of("http_proxy", proxyUrl))
                .build();

        server.start();

        // This test needs to be done via `server` mode and we can't inject recordable TDClient to Digdag
        // through utils.TestUtils.RecordableWorkflow for now.
        //
        // So this test should use `td_store_last_result.dig` instead
        // so that we can check the output file and confirm `td` operator worked fine.
        copyResource("acceptance/td/td/td_store_last_result.dig", projectDir.resolve("workflow.dig"));

        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "td.apikey", TD_API_KEY);

        Id attemptId = pushAndStart(server.endpoint(), projectDir, "workflow", ImmutableMap.of(
                "outfile", outfile.toString(),
                "td.use_ssl", "true"));

        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));

        assertOutputOfTdStoreLastResult();
    }

    @Test
    public void testRunQueryOnServerWithoutSecretAccessPolicy()
            throws Exception
    {
        TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .configuration("digdag.secret-encryption-key = " + ENCRYPTION_KEY)
                .build();

        server.start();

        copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));

        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "td.apikey", TD_API_KEY);

        Id attemptId = pushAndStart(server.endpoint(), projectDir, "workflow", ImmutableMap.of(
                "outfile", outfile.toString(),
                "td.use_ssl", "true"));

        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
    }

    @Test
    public void testRunQueryWithPreview()
            throws Exception
    {
        copyResource("acceptance/td/td/td_preview.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        assertWorkflowRunsSuccessfully();
    }

    @Test
    public void testRunQueryWithPreviewAndCreateTable()
            throws Exception
    {
        copyResource("acceptance/td/td/td_preview_create_table.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        assertWorkflowRunsSuccessfully("td.database=" + database);
    }

    @Test
    public void verifyApikeyParamIsNotUsed()
            throws Exception
    {
        // Replace the "secrets.td.apikey" entry with the legacy "params.td.apikey" entry
        Files.write(config, asList("params.td.apikey = " + TD_API_KEY));

        copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));

        CommandStatus status = runWorkflow().commandStatus;

        assertThat(status.errUtf8(), Matchers.containsString("The 'td.apikey' secret is missing"));
    }

    @Test
    public void testRetryAndTryUpdateApikeyWithInvalidApikey()
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        try {
            Files.write(config, asList("secrets.td.apikey = " + "dummy"));

            copyResource("acceptance/td/td/td.dig", projectDir.resolve("workflow.dig"));
            copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));

            runWorkflow();

            assertThat(out.toString(), Matchers.containsString("apikey will be tried to update by retrying"));
        } finally {
            System.setOut(System.out);
        }
    }

    @Test
    public void testRunQueryInline()
            throws Exception
    {
        copyResource("acceptance/td/td/td_inline.dig", projectDir.resolve("workflow.dig"));
        assertWorkflowRunsSuccessfully();
    }

    @Test
    public void testRunResultUrlSecret()
            throws Exception
    {
        copyResource("acceptance/td/td/td_result_url.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td/query.sql", projectDir.resolve("query.sql"));
        assertWorkflowRunsSuccessfully("td.database=" + database);
    }

    @Test
    public void testRetries()
            throws Exception
    {
        int failures = 7;

        ConcurrentMap<String, List<FullHttpRequest>> requests = new ConcurrentHashMap<>();

        proxyServer = TestUtils.startRequestFailingProxy(failures, requests);

        // In this test, the http proxy basically passes all requests to real TD API.
        // So `td` operator needs to use HTTPS not HTTP.
        Files.write(config, asList(
                "config.td.min_retry_interval = 1s",
                "config.td.max_retry_interval = 1s",
                "params.td.use_ssl = true",
                "params.td.proxy.enabled = true",
                "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
        ), APPEND);

        copyResource("acceptance/td/td/td_inline.dig", projectDir.resolve("workflow.dig"));
        List<ApiCallRecord> apiCallRecords = assertWorkflowRunsSuccessfullyAndReturnApiCalls().apiCallRecords;

        for (Map.Entry<String, List<FullHttpRequest>> entry : requests.entrySet()) {
            System.err.println(entry.getKey() + ": " + entry.getValue().size());
        }

        for (List<FullHttpRequest> reqs : requests.values()) {
            reqs.forEach(ReferenceCountUtil::releaseLater);
        }

        // Verify that all requests were retried
        for (Map.Entry<String, List<FullHttpRequest>> entry : requests.entrySet()) {
            String key = entry.getKey();
            List<FullHttpRequest> keyedRequests = entry.getValue();
            assertThat(key, keyedRequests.size(), Matchers.is(Matchers.greaterThanOrEqualTo(failures)));
        }

        List<TDApiRequest> jobIssueRequests = apiCallRecords.stream()
                .filter(e -> e.request.getPath().contains("/v3/job/issue"))
                .map(record -> record.request)
                .collect(toList());

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
                "params.td.use_ssl = true",
                "params.td.proxy.enabled = true",
                "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
        ), APPEND);

        copyResource("acceptance/td/td/td_inline.dig", projectDir.resolve("workflow.dig"));
        List<TDApiRequest> recordedIssueApiCalls = assertWorkflowRunsSuccessfullyAndReturnApiCalls().apiCallRecords.stream()
                .filter(req -> req.request.getPath().contains("/v3/job/issue"))
                .map(record -> record.request)
                .collect(toList());

        for (FullHttpRequest request : jobIssueRequests) {
            ReferenceCountUtil.releaseLater(request);
        }

        assertThat(recordedIssueApiCalls.size(), is(not(0)));
        assertThat(recordedIssueApiCalls.size(), is(not(0)));

        verifyDomainKeys(recordedIssueApiCalls);
    }

    private void verifyDomainKeys(List<TDApiRequest> requests)
            throws IOException
    {
        // Verify that all job issue requests reuse the same domain key
        TDApiRequest firstRequest = requests.get(0);
        String domainKey = domainKey(firstRequest);

        for (int i = 0; i < requests.size(); i++) {
            TDApiRequest request = requests.get(i);
            String requestDomainKey = domainKey(request);
            assertThat(requestDomainKey, is(domainKey));
        }
    }

    private String domainKey(TDApiRequest request)
    {
        return request.getQueryParams().get("domain_key");
    }

    private CommandStatus assertWorkflowRunsSuccessfully(String... params)
    {
        CommandStatus runStatus = runWorkflow(params).commandStatus;
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(Files.exists(outfile), is(true));
        return runStatus;
    }

    private CommandStatusAndRecordedApiCalls assertWorkflowRunsSuccessfullyAndReturnApiCalls(String... params)
    {
        CommandStatusAndRecordedApiCalls result = runWorkflow(params);
        CommandStatus runStatus = result.commandStatus;
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(Files.exists(outfile), is(true));
        return result;
    }

    private CommandStatusAndRecordedApiCalls runWorkflow(String... params)
    {
        List<String> args = new ArrayList<>();
        // `mainWithRecordableRun()` below introduces `recordable_run` command
        // which is extended `run` command and records all TD API calls
        args.addAll(asList("recordable_run",
                "-o", projectDir.toString(),
                "--log-level", "debug",
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "outfile=" + outfile));

        for (String param : params) {
            args.add("-p");
            args.add(param);
        }

        args.add("workflow.dig");

        CommandStatusAndRecordedApiCalls commandStatusAndRecords = mainWithRecordableRun(env, args);

        return commandStatusAndRecords;
    }
}
