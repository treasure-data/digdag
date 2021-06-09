package acceptance.td;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.BiFunction;

import static acceptance.td.GcpUtil.GCP_CREDENTIAL;
import static acceptance.td.GcpUtil.GCS_PREFIX;
import static acceptance.td.GcpUtil.GCS_TEST_BUCKET;
import static java.nio.charset.StandardCharsets.UTF_8;
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

public class GcsWaitIT
{
    private static final ObjectMapper MAPPER = DigdagClient.objectMapper();

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    private Path projectDir;
    private String projectName;
    private Id projectId;

    private Path outfile;

    private DigdagClient digdagClient;

    private HttpProxyServer proxyServer;
    private GoogleCredential gcpCredential;
    private JsonFactory jsonFactory;
    private HttpTransport transport;
    private Storage gcs;
    private String gcpProjectId;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(GCP_CREDENTIAL, not(isEmptyOrNullString()));
        assertThat(GCS_TEST_BUCKET, not(isEmptyOrNullString()));

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

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
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
    public void tearDown()
            throws Exception
    {
        if (transport != null) {
            transport.shutdown();
            transport = null;
        }
    }

    public static class CommandPathIT
            extends GcsWaitIT
    {
        @Test
        public void testGcsWaitPath()
                throws Exception
        {
            testGcsWait("gcs_wait_path", (bucket, object) -> "gcs_wait>: " + bucket + "/" + object);
        }
    }

    public static class CommandUriIT
            extends GcsWaitIT
    {
        @Test
        public void testGcsWaitUri()
                throws Exception
        {
            testGcsWait("gcs_wait_uri", (bucket, object) -> "gcs_wait>: gs://" + bucket + "/" + object);
        }
    }

    public static class ParamIT
            extends GcsWaitIT
    {
        @Test
        public void testGcsWaitBucketObject()
                throws Exception
        {
            testGcsWait("gcs_wait_bucket_object", (bucket, object) -> "gcs_wait>: ");
        }
    }

    protected void testGcsWait(String workflow, BiFunction<String, String, String> logNeedle)
            throws Exception
    {
        String objectName = GCS_PREFIX + "data.csv";

        // Start waiting
        addWorkflow(projectDir, "acceptance/gcs_wait/" + workflow + ".dig");
        Id attemptId = pushAndStart(server.endpoint(), projectDir, workflow, ImmutableMap.of(
                "test_bucket", GCS_TEST_BUCKET,
                "test_object", objectName,
                "outfile", outfile.toString()));

        // Wait for gcs_wait polling to show up in logs
        expect(Duration.ofSeconds(30), () -> {
            String attemptLogs = TestUtils.getAttemptLogs(client, attemptId);
            return attemptLogs.contains(logNeedle.apply(GCS_TEST_BUCKET, objectName));
        });

        // Verify that the dependent task has not been executed
        assertThat(Files.exists(outfile), is(false));

        // Verify that the attempt is not yet done
        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getDone(), is(false));

        // Create object
        byte[] data = "hello gcs!".getBytes(UTF_8);
        InputStreamContent content = new InputStreamContent("text/plain", new ByteArrayInputStream(data))
                .setLength(data.length);
        StorageObject metadata = new StorageObject().setName(objectName);
        gcs.objects()
                .insert(GCS_TEST_BUCKET, metadata, content)
                .execute();

        // Expect the attempt to finish and the dependent task to be executed
        expect(Duration.ofSeconds(300), attemptSuccess(server.endpoint(), attemptId));
        assertThat(Files.exists(outfile), is(true));

        JsonNode objectMetadata = MAPPER.readTree(Files.readAllBytes(outfile));
        int size = objectMetadata.get("metadata").get("size").asInt();
        assertThat(size, is(data.length));
    }
}
