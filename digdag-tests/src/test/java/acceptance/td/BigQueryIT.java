package acceptance.td;

import com.amazonaws.util.StringInputStream;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.pushProject;

public class BigQueryIT
{
    private static final String GCP_CREDENTIAL = System.getenv().getOrDefault("GCP_CREDENTIAL", "");
    private static final String GCS_TEST_BUCKET = System.getenv().getOrDefault("GCS_TEST_BUCKET", "");

    private static final String GCS_PREFIX = "test/" + UUID.randomUUID().toString().replace('-', '_') + '/';
    private static final String BQ_TAG = "test_" + UUID.randomUUID().toString().replace('-', '_');

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    private Path projectDir;
    private String projectName;
    private int projectId;

    private Path outfile;

    private DigdagClient digdagClient;

    private HttpProxyServer proxyServer;
    private GoogleCredential gcpCredential;
    private JsonFactory jsonFactory;
    private HttpTransport transport;
    private Storage gcs;
    private Bigquery bq;
    private String gcpProjectId;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(GCP_CREDENTIAL, not(isEmptyOrNullString()));

        proxyServer = TestUtils.startRequestFailingProxy(1);

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

        gcpProjectId = DigdagClient.objectMapper().readTree(GCP_CREDENTIAL).get("project_id").asText();
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
        if (gcs == null) {
            return;
        }

        Storage.Objects.List req = gcs.objects().list(GCS_TEST_BUCKET);
        Objects objects;
        do {
            objects = req.execute();
            List<StorageObject> items = objects.getItems();
            if (items != null) {
                for (StorageObject object : items) {
                    if (object.getName().startsWith(GCS_PREFIX)) {
                        try {
                            gcs.objects().delete(GCS_TEST_BUCKET, object.getName()).execute();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            req.setPageToken(objects.getNextPageToken());
        }
        while (null != objects.getNextPageToken());
    }

    @After
    public void cleanupBq()
            throws Exception
    {
        if (bq == null) {
            return;
        }
        List<DatasetList.Datasets> datasets = listAllDatasets(
                bq, gcpProjectId, ds -> ds.getDatasetReference().getDatasetId().contains(BQ_TAG));
        for (DatasetList.Datasets dataset : datasets) {
            deleteDataset(bq, gcpProjectId, dataset.getDatasetReference().getDatasetId());
        }
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

    @Test
    public void testQuery()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/bigquery/query.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "query", ImmutableMap.of("outfile", outfile.toString()));
        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
        assertThat(Files.exists(outfile), is(true));
    }

    @Test
    public void testLoad()
            throws Exception
    {
        assumeThat(GCS_TEST_BUCKET, not(isEmptyOrNullString()));

        // Create source data object
        String objectName = GCS_PREFIX + "test.csv";
        byte[] data = Joiner.on('\n').join("a,b", "c,d").getBytes(UTF_8);
        InputStreamContent content = new InputStreamContent("text/csv", new ByteArrayInputStream(data))
                .setLength(data.length);
        StorageObject metadata = new StorageObject().setName(objectName);
        gcs.objects()
                .insert(GCS_TEST_BUCKET, metadata, content)
                .execute();

        // Create output dataset
        String datasetId = BQ_TAG + "_loadtest";
        Dataset dataset = new Dataset().setDatasetReference(new DatasetReference()
                .setProjectId(gcpProjectId)
                .setDatasetId(datasetId));
        bq.datasets().insert(gcpProjectId, dataset)
                .execute();

        // Run load
        String tableId = "data";
        addWorkflow(projectDir, "acceptance/bigquery/load.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "load", ImmutableMap.of(
                "source_bucket", GCS_TEST_BUCKET,
                "source_object", objectName,
                "target_dataset", datasetId,
                "target_table", tableId,
                "outfile", outfile.toString()));
        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
        assertThat(Files.exists(outfile), is(true));

        // Check that destination table was created
        Table destinationTable = bq.tables().get(gcpProjectId, datasetId, tableId).execute();
        assertThat(destinationTable.getId(), is(tableId));
    }

    private static List<TableList.Tables> listAllTables(Bigquery bq, String projectId, String datasetId, Predicate<TableList.Tables> needle)
            throws IOException
    {
        List<TableList.Tables> tables = new ArrayList<>();
        Bigquery.Tables.List req = bq.tables().list(projectId, datasetId);
        TableList tableList;
        do {
            tableList = req.execute();
            if (tableList.getTables() != null) {
                tableList.getTables().stream().filter(needle).forEach(tables::add);
            }
            req.setPageToken(tableList.getNextPageToken());
        }
        while (null != tableList.getNextPageToken());
        return tables;
    }

    private static List<DatasetList.Datasets> listAllDatasets(Bigquery bq, String projectId, Predicate<DatasetList.Datasets> needle)
            throws IOException
    {
        List<DatasetList.Datasets> datasets = new ArrayList<>();
        Bigquery.Datasets.List req = bq.datasets().list(projectId);
        DatasetList datasetList;
        do {
            datasetList = req.execute();
            if (datasetList.getDatasets() != null) {
                datasetList.getDatasets().stream()
                        .filter(needle)
                        .forEach(datasets::add);
            }
            req.setPageToken(datasetList.getNextPageToken());
        }
        while (null != datasetList.getNextPageToken());
        return datasets;
    }

    private static void deleteDataset(Bigquery bq, String gcpProjectId, String datasetId)
            throws IOException
    {
        // Delete tables
        List<TableList.Tables> tables = listAllTables(bq, gcpProjectId, datasetId, table -> true);
        for (TableList.Tables table : tables) {
            String tableId = table.getTableReference().getTableId();
            try {
                bq.tables().delete(gcpProjectId, datasetId, tableId).execute();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Delete dataset
        try {
            bq.datasets().delete(gcpProjectId, datasetId).execute();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
