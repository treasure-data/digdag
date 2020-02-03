package acceptance.td;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.netty.handler.codec.http.FullHttpRequest;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static acceptance.td.Secrets.TD_API_KEY;
import static acceptance.td.Secrets.TD_API_ENDPOINT;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.objectMapper;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.startRequestTrackingProxy;

public class TdResultExportIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TDClient client;
    private String database;
    private String table;
    private String connectionId;
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private OkHttpClient httpClient;
    private Path projectDir;

    public TemporaryDigdagServer server;

    private HttpProxyServer proxyServer;
    private String sampleJobId;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        assumeThat(TD_API_ENDPOINT, not(isEmptyOrNullString()));
        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .setEndpoint(TD_API_ENDPOINT)
                .build();
        sampleJobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets",
                "select time from www_access"));
        database = "_digdag_integration_result_export_td_test_db";
        client.createDatabaseIfNotExists(database);
        table = "_digdag_integration_result_export_td_test_table";
        client.createTableIfNotExists(database, table);
        this.httpClient = new OkHttpClient();
    }

    @Test
    public void testSubmitResultExportJob()
            throws Exception
    {
        String resultConnectorName =  "digdag_test_" + UUID.randomUUID().toString().replace('-', '_');
        String json = "{\"description\":null,\"name\":\"" + resultConnectorName + "\"," +
                "\"settings\":{\"api_key\":\"\",\"api_hostname\":\"\"}," +
                "\"shared\":false,\"type\":\"treasure_data\"}";

        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url("https://" + TD_API_ENDPOINT + "/v4/connections")
                .header("authorization", "TD1 " + TD_API_KEY)
                .post(body)
                .build();
        Response response = httpClient.newCall(request).execute();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(response.body().string());
        connectionId = jsonNode.get("id").asText();

        List<FullHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

        addWorkflow(projectDir, "acceptance/td/td_result_export/td_result_export.dig");

        proxyServer = startRequestTrackingProxy(requests);

        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();

        TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .configuration(Secrets.secretsServerConfiguration())
                .environment(ImmutableMap.of("http_proxy", proxyUrl))
                .build();

        server.start();

        copyResource("acceptance/td/td_result_export/td_result_export.dig", projectDir.resolve("td_result_export.dig"));
        TestUtils.addWorkflow(projectDir, "acceptance/td/td_result_export/td_result_export.dig");

        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        DigdagClient digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "td.apikey", TD_API_KEY);

        Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_result_export", ImmutableMap.of(
                "test_job_id", sampleJobId,
                "test_result_settings", "{\"user_database_name\":\""+database+"\",\"user_table_name\":\""+table+"\",\"mode\":\"replace\"}",
                "test_result_connection", resultConnectorName,
                "td.use_ssl", "false")
        );

        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));

        String selectCountJobId = client.submit(TDJobRequest.newPrestoQuery(database, "select count(*) from " + table));
        TestUtils.expect(Duration.ofMinutes(5), jobSuccess(client, selectCountJobId));

        List<ArrayNode> result = downloadResult(selectCountJobId);
        assertThat(result.get(0).get(0).asInt(), is(5000));
    }

    private List<ArrayNode> downloadResult(String jobId)
    {
        return client.jobResult(jobId, TDResultFormat.JSON, input -> {
            try {
                List<String> lines = CharStreams.readLines(new InputStreamReader(input));
                ObjectReader reader = objectMapper().readerFor(ArrayNode.class);
                List<ArrayNode> result = new ArrayList<>();
                for (String line : lines) {
                    result.add(reader.readValue(line));
                }
                return result;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    private static Callable<Boolean> jobSuccess(TDClient client, String jobId)
    {
        return () -> {
            TDJobSummary status = client.jobStatus(jobId);
            if (status.getStatus() == TDJob.Status.SUCCESS) {
                return true;
            }
            if (status.getStatus().isFinished()) {
                fail(status.getStatus().toString());
            }
            return false;
        };
    }

    @After
    public void deleteConnection()
            throws IOException
    {
        if(connectionId != null){
            Request request = new Request.Builder()
                    .url("https://" + TD_API_ENDPOINT + "/v4/connections/" + connectionId)
                    .header("authorization", "TD1 " + TD_API_KEY)
                    .delete()
                    .build();
            httpClient.newCall(request).execute();
        }
    }
}
