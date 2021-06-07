package acceptance.td;

import com.amazonaws.util.StringInputStream;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.util.RetryExecutor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static acceptance.td.GcpUtil.BQ_TAG;
import static acceptance.td.GcpUtil.GCP_CREDENTIAL;
import static acceptance.td.GcpUtil.GCS_PREFIX;
import static acceptance.td.GcpUtil.GCS_TEST_BUCKET;
import static acceptance.td.GcpUtil.createDataset;
import static acceptance.td.GcpUtil.createTable;
import static acceptance.td.GcpUtil.datasetExists;
import static acceptance.td.GcpUtil.listTables;
import static acceptance.td.GcpUtil.tableExists;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.pushProject;
import static io.digdag.util.RetryExecutor.retryExecutor;

public class BigQueryIT
{
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    protected Path projectDir;
    protected String projectName;
    protected Id projectId;

    protected Path outfile;

    protected DigdagClient digdagClient;

    protected HttpProxyServer proxyServer;
    protected GoogleCredential gcpCredential;
    protected JsonFactory jsonFactory;
    protected HttpTransport transport;
    protected Storage gcs;
    protected Bigquery bq;
    protected String gcpProjectId = GcpUtil.GCP_PROJECT_ID;

    private static RetryExecutor retryExecutor = retryExecutor()
            .withInitialRetryWait(500)
            .withMaxRetryWait(2000)
            .withRetryLimit(3)
            // retry for 5xx status code
            .retryIf((e) -> e instanceof HttpResponseException && ((HttpResponseException) e).getStatusCode() >= 500);

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(GCP_CREDENTIAL, not(isEmptyOrNullString()));

        proxyServer = TestUtils.startRequestFailingProxy(2, new ConcurrentHashMap<>(), HttpResponseStatus.INTERNAL_SERVER_ERROR,
                (req, reqCount) -> {
                    // io.digdag.standards.operator.gcp.BqJobRunner sends "CONNECT www.googleapis.com" frequently. It can easily cause infinite retry.
                    // So the following custom logic should be used for that kind of requests.
                    if (req.getMethod().equals(HttpMethod.CONNECT)) {
                        return Optional.of(reqCount % 5 == 0);
                    }
                    return Optional.absent();
                });

        server = TemporaryDigdagServer.builder()
                .environment(ImmutableMap.of(
                        "https_proxy", "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort())
                )
                .withRandomSecretEncryptionKey()
                .build();
        server.start();

        projectDir = folder.getRoot().toPath();
        createProject(projectDir);
        projectName = projectDir.getFileName().toString();
        projectId = pushProject(server.endpoint(), projectDir, projectName);

        outfile = folder.newFolder().toPath().resolve("outfile");

        digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "gcp.credential", GCP_CREDENTIAL);

        gcpCredential = GoogleCredential.fromStream(new StringInputStream(GCP_CREDENTIAL));

        assertThat(gcpProjectId, not(isEmptyOrNullString()));

        jsonFactory = new JacksonFactory();
        transport = GoogleNetHttpTransport.newTrustedTransport();
        gcs = gcsClient(gcpCredential);
        bq = bqClient(gcpCredential);
    }

    private Storage gcsClient(GoogleCredential credential)
    {
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(StorageScopes.all());
        }
        return new Storage.Builder(transport, jsonFactory, credential)
                .setApplicationName("digdag-test")
                .build();
    }

    private Bigquery bqClient(GoogleCredential credential)
    {
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("digdag-test")
                .build();
    }

    @After
    public void tearDownDigdagServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @After
    public void tearDownProxyServer()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
    }

    @After
    public void cleanupGcs()
            throws Exception
    {
        GcpUtil.cleanupGcs(gcs);
    }

    @After
    public void cleanupBq()
            throws Exception
    {
        GcpUtil.cleanupBq(bq, gcpProjectId);
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (transport != null) {
            transport.shutdown();
            transport = null;
        }
    }

    public static class QueryIT
            extends BigQueryIT
    {

        @Test
        public void testQuery()
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/bigquery/query.dig");
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "query", ImmutableMap.of("outfile", outfile.toString()));
            expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));
        }
    }

    public static class LoadIT
            extends BigQueryIT
    {

        @Test
        public void testLoad()
                throws Exception
        {
            assertThat(GCS_TEST_BUCKET, not(isEmptyOrNullString()));

            // Create source data object
            String objectName = GCS_PREFIX + "test.csv";
            byte[] data = Joiner.on('\n').join("a,b", "c,d").getBytes(UTF_8);
            InputStreamContent content = new InputStreamContent("text/csv", new ByteArrayInputStream(data))
                    .setLength(data.length);
            StorageObject metadata = new StorageObject().setName(objectName);
            retryExecutor.run(() -> gcs.objects()
                    .insert(GCS_TEST_BUCKET, metadata, content)
                    .execute());

            // Create output dataset
            String datasetId = BQ_TAG + "_load_test";
            Dataset dataset = new Dataset().setDatasetReference(new DatasetReference()
                    .setProjectId(gcpProjectId)
                    .setDatasetId(datasetId));
            retryExecutor.run(() -> bq.datasets().insert(gcpProjectId, dataset)
                    .execute());

            // Run load
            String tableId = "data";
            addWorkflow(projectDir, "acceptance/bigquery/load.dig");
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "load", ImmutableMap.of(
                    "source_bucket", GCS_TEST_BUCKET,
                    "source_object", objectName,
                    "target_dataset", datasetId,
                    "target_table", tableId,
                    "outfile", outfile.toString()));
            expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));

            // Check that destination table was created
            Table destinationTable = retryExecutor.run(() -> bq.tables().get(gcpProjectId, datasetId, tableId).execute());
            assertThat(destinationTable.getTableReference().getTableId(), is(tableId));
        }
    }

    public static class ExtractIT
            extends BigQueryIT
    {
        @Test
        public void testExtract()
                throws Exception
        {
            assertThat(GCS_TEST_BUCKET, not(isEmptyOrNullString()));

            // Create source table
            String tableId = "data";
            String datasetId = BQ_TAG + "_extract_test";
            Dataset dataset = new Dataset().setDatasetReference(new DatasetReference()
                    .setProjectId(gcpProjectId)
                    .setDatasetId(datasetId));
            retryExecutor.run(() -> bq.datasets().insert(gcpProjectId, dataset)
                    .execute());
            Table table = new Table().setTableReference(new TableReference()
                    .setProjectId(gcpProjectId)
                    .setTableId(tableId))
                    .setSchema(new TableSchema()
                            .setFields(ImmutableList.of(
                                    new TableFieldSchema().setName("foo").setType("STRING"),
                                    new TableFieldSchema().setName("bar").setType("STRING")
                            )));
            retryExecutor.run(() -> bq.tables().insert(gcpProjectId, datasetId, table)
                    .execute());

            // Populate source table
            TableDataInsertAllRequest content = new TableDataInsertAllRequest()
                    .setRows(ImmutableList.of(
                            new TableDataInsertAllRequest.Rows().setJson(ImmutableMap.of(
                                    "foo", "a",
                                    "bar", "b")),
                            new TableDataInsertAllRequest.Rows().setJson(ImmutableMap.of(
                                    "foo", "c",
                                    "bar", "d"))));
            retryExecutor.run(() -> bq.tabledata().insertAll(gcpProjectId, datasetId, tableId, content)
                    .execute());

            // Run extract
            String objectName = GCS_PREFIX + "test.csv";
            addWorkflow(projectDir, "acceptance/bigquery/extract.dig");
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "extract", ImmutableMap.of(
                    "src_dataset", datasetId,
                    "src_table", tableId,
                    "dst_bucket", GCS_TEST_BUCKET,
                    "dst_object", objectName,
                    "outfile", outfile.toString()));
            expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));

            // Check that destination file was created
            StorageObject metadata = retryExecutor.run(() -> gcs.objects().get(GCS_TEST_BUCKET, objectName)
                    .execute());
            assertThat(metadata.getName(), is(objectName));
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            retryExecutor.run(() -> {
                try {
                    gcs.objects().get(GCS_TEST_BUCKET, objectName)
                            .executeMediaAndDownloadTo(data);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            });
        }
    }

    public static class DdlIT
            extends BigQueryIT
    {

        @Test
        public void testDdl()
                throws Exception
        {
            String testCreateDataset1 = BQ_TAG + "_create_dataset_1";
            String testCreateDataset2 = BQ_TAG + "_create_dataset_2";
            String testDeleteDataset1 = BQ_TAG + "_delete_dataset_1";
            String testDeleteDataset2 = BQ_TAG + "_delete_dataset_2";
            String testEmptyDataset1 = BQ_TAG + "_empty_dataset_1";
            String testEmptyDataset2 = BQ_TAG + "_empty_dataset_2";
            String testDeleteDataset1Table1 = "test_delete_dataset_1_table_1";
            String testEmptyDataset1Table1 = "test_empty_dataset_1_table_1";
            String testDefaultDataset = BQ_TAG + "_default_dataset";
            String testDeleteTable2ExistingDataset = BQ_TAG + "_delete_table_2_existing_dataset";
            String testCreateTable2EmptyDataset = BQ_TAG + "_create_table_2_empty_dataset";
            String testCreateTable3CreateDataset = BQ_TAG + "_create_table_3_create_dataset";
            String testCreateTable4ExistingDataset = BQ_TAG + "_create_table_4_existing_dataset";
            String testCreateTable1 = "test_create_table_1";
            String testCreateTable2 = "test_create_table_2";
            String testCreateTable3 = "test_create_table_3";
            String testCreateTable4 = "test_create_table_4";
            String testCreateTable5 = "test_create_table_5";
            String testDeleteTable1 = "test_delete_table_1";
            String testDeleteTable2 = "test_delete_table_2";
            String testEmptyTable2EmptyDataset = BQ_TAG + "_empty_table_2_empty_dataset";
            String testEmptyTable3CreateDataset = BQ_TAG + "_empty_table_3_create_dataset";
            String testEmptyTable4ExistingDataset = BQ_TAG + "_empty_table_4_existing_dataset";
            String testEmptyTable1 = "test_empty_table_1";
            String testEmptyTable2 = "test_empty_table_2";
            String testEmptyTable3 = "test_empty_table_3";
            String testEmptyTable4 = "test_empty_table_4";
            String testEmptyTable5 = "test_empty_table_5";

            createDataset(bq, gcpProjectId, testDefaultDataset);
            createDataset(bq, gcpProjectId, testDeleteDataset1);
            createDataset(bq, gcpProjectId, testEmptyDataset1);
            createDataset(bq, gcpProjectId, testCreateTable4ExistingDataset);
            createDataset(bq, gcpProjectId, testEmptyTable4ExistingDataset);
            createDataset(bq, gcpProjectId, testDeleteTable2ExistingDataset);

            createTable(bq, gcpProjectId, testDeleteDataset1, testDeleteDataset1Table1);
            createTable(bq, gcpProjectId, testEmptyDataset1, testEmptyDataset1Table1);
            createTable(bq, gcpProjectId, testDefaultDataset, testDeleteTable1);
            createTable(bq, gcpProjectId, testDeleteTable2ExistingDataset, testDeleteTable2);

            createTable(bq, gcpProjectId, testDefaultDataset, new Table()
                    .setTableReference(new TableReference()
                            .setProjectId(gcpProjectId)
                            .setDatasetId(testDefaultDataset)
                            .setTableId(testEmptyTable5))
                    .setSchema(new TableSchema()
                            .setFields(ImmutableList.of(
                                    new TableFieldSchema().setName("f1").setType("STRING"),
                                    new TableFieldSchema().setName("f2").setType("STRING")
                            ))));

            retryExecutor.run(() ->
                    bq.tabledata().insertAll(gcpProjectId, testDefaultDataset, testEmptyTable5, new TableDataInsertAllRequest()
                            .setRows(ImmutableList.of(
                                    new TableDataInsertAllRequest.Rows().setJson(ImmutableMap.of("f1", "v1a", "f2", "v2a")),
                                    new TableDataInsertAllRequest.Rows().setJson(ImmutableMap.of("f1", "v1b", "f2", "v2b"))
                                    )
                            )
                    )
            );

            addWorkflow(projectDir, "acceptance/bigquery/ddl.dig");
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "ddl", ImmutableMap.<String, String>builder()
                    .put("test_default_dataset", testDefaultDataset)
                    .put("test_create_dataset_1", testCreateDataset1)
                    .put("test_create_dataset_2", testCreateDataset2)
                    .put("test_delete_dataset_1", testDeleteDataset1)
                    .put("test_delete_dataset_2", testDeleteDataset2)
                    .put("test_empty_dataset_1", testEmptyDataset1)
                    .put("test_empty_dataset_2", testEmptyDataset2)
                    .put("test_create_table_1", testCreateTable1)
                    .put("test_create_table_2", testCreateTable2)
                    .put("test_create_table_3", testCreateTable3)
                    .put("test_create_table_4", testCreateTable4)
                    .put("test_create_table_5", testCreateTable5)
                    .put("test_create_table_2_empty_dataset", testCreateTable2EmptyDataset)
                    .put("test_create_table_3_create_dataset", testCreateTable3CreateDataset)
                    .put("test_create_table_4_existing_dataset", testCreateTable4ExistingDataset)
                    .put("test_delete_table_2_dataset", testDeleteTable2ExistingDataset)
                    .put("test_delete_table_1", testDeleteTable1)
                    .put("test_delete_table_2", testDeleteTable2)
                    .put("test_empty_table_2_empty_dataset", testEmptyTable2EmptyDataset)
                    .put("test_empty_table_3_create_dataset", testEmptyTable3CreateDataset)
                    .put("test_empty_table_4_existing_dataset", testEmptyTable4ExistingDataset)
                    .put("test_empty_table_1", testEmptyTable1)
                    .put("test_empty_table_2", testEmptyTable2)
                    .put("test_empty_table_3", testEmptyTable3)
                    .put("test_empty_table_4", testEmptyTable4)
                    .put("test_empty_table_5", testEmptyTable5)
                    .put("outfile", outfile.toString())
                    .build());

            expect(Duration.ofMinutes(10), attemptSuccess(server.endpoint(), attemptId), Duration.ofSeconds(20));

            assertThat(Files.exists(outfile), is(true));

            assertThat(datasetExists(bq, gcpProjectId, testCreateDataset1), is(true));
            assertThat(datasetExists(bq, gcpProjectId, testCreateDataset2), is(true));
            assertThat(datasetExists(bq, gcpProjectId, testEmptyDataset1), is(true));
            assertThat(datasetExists(bq, gcpProjectId, testEmptyDataset2), is(true));
            assertThat(datasetExists(bq, gcpProjectId, testDeleteDataset1), is(false));
            assertThat(datasetExists(bq, gcpProjectId, testDeleteDataset2), is(false));

            assertThat(listTables(bq, gcpProjectId, testEmptyDataset1), is(empty()));
            assertThat(listTables(bq, gcpProjectId, testEmptyDataset2), is(empty()));

            assertThat(tableExists(bq, gcpProjectId, testDefaultDataset, testCreateTable1), is(true));
            assertThat(tableExists(bq, gcpProjectId, testCreateTable2EmptyDataset, testCreateTable2), is(true));
            assertThat(tableExists(bq, gcpProjectId, testCreateTable3CreateDataset, testCreateTable3), is(true));
            assertThat(tableExists(bq, gcpProjectId, testCreateTable4ExistingDataset, testCreateTable4), is(true));
            assertThat(tableExists(bq, gcpProjectId, testDefaultDataset, testCreateTable5), is(true));

            assertThat(tableExists(bq, gcpProjectId, testDefaultDataset, testDeleteTable1), is(false));
            assertThat(tableExists(bq, gcpProjectId, testDeleteTable2ExistingDataset, testDeleteTable2), is(false));

            assertThat(tableExists(bq, gcpProjectId, testDefaultDataset, testEmptyTable1), is(true));
            assertThat(tableExists(bq, gcpProjectId, testEmptyTable2EmptyDataset, testEmptyTable2), is(true));
            assertThat(tableExists(bq, gcpProjectId, testEmptyTable3CreateDataset, testEmptyTable3), is(true));
            assertThat(tableExists(bq, gcpProjectId, testEmptyTable4ExistingDataset, testEmptyTable4), is(true));
            assertThat(tableExists(bq, gcpProjectId, testDefaultDataset, testEmptyTable5), is(true));

            Table emptyTable5 = bq.tables().get(gcpProjectId, testDefaultDataset, testEmptyTable5).execute();
            assertThat(emptyTable5.getNumRows(), is(BigInteger.ZERO));
        }
    }
}
