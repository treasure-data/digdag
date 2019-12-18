package acceptance;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

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
import static utils.TestUtils.startWorkflow;

public class S3WaitIT
{
    private static Logger logger = LoggerFactory.getLogger(S3WaitIT.class);

    private static final String TEST_S3_ENDPOINT = System.getenv("TEST_S3_ENDPOINT");
    private static final String TEST_S3_ACCESS_KEY_ID = System.getenv().getOrDefault("TEST_S3_ACCESS_KEY_ID", "test");
    private static final String TEST_S3_SECRET_ACCESS_KEY = System.getenv().getOrDefault("TEST_S3_SECRET_ACCESS_KEY", "test");

    private static final ObjectMapper MAPPER = DigdagClient.objectMapper();

    public TemporaryDigdagServer server;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path projectDir;
    private DigdagClient client;
    private HttpProxyServer proxyServer;
    private String bucket;

    private AmazonS3 s3;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TEST_S3_ENDPOINT, not(isEmptyOrNullString()));

        proxyServer = TestUtils.startRequestFailingProxy(10);

        server = TemporaryDigdagServer.builder()
                .environment(ImmutableMap.of(
                        "http_proxy", "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort())
                )
                .configuration(
                        "digdag.secret-encryption-key = " + Base64.getEncoder().encodeToString(RandomUtils.nextBytes(16)))
                .build();

        server.start();

        projectDir = folder.getRoot().toPath().resolve("foobar");

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        bucket = UUID.randomUUID().toString();

        AWSCredentials credentials = new BasicAWSCredentials(TEST_S3_ACCESS_KEY_ID, TEST_S3_SECRET_ACCESS_KEY);
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(TEST_S3_ENDPOINT, null))
                .build();
        s3.createBucket(bucket);
    }

    @After
    public void tearDownProxy()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
    }

    @After
    public void tearDownServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void testRun()
            throws Exception
    {
        String key = UUID.randomUUID().toString();


        Path outfile = folder.newFolder().toPath().resolve("out");

        createProject(projectDir);
        addWorkflow(projectDir, "acceptance/s3/s3_wait.dig");

        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        // Configure AWS credentials
        client.setProjectSecret(projectId, "aws.s3.access_key_id", TEST_S3_ACCESS_KEY_ID);
        client.setProjectSecret(projectId, "aws.s3.secret_access_key", TEST_S3_SECRET_ACCESS_KEY);
        client.setProjectSecret(projectId, "aws.s3.endpoint", TEST_S3_ENDPOINT);

        // Start workflow
        String projectName = projectDir.getFileName().toString();
        Id attemptId = startWorkflow(server.endpoint(), projectName, "s3_wait", ImmutableMap.of(
                "path", bucket + "/" + key,
                "outfile", outfile.toString()
        ));

        // Wait for s3 polling to show up in logs
        expect(Duration.ofSeconds(30), () -> {
            String attemptLogs = TestUtils.getAttemptLogs(client, attemptId);
            return attemptLogs.contains("s3_wait>: " + bucket + "/" + key);
        });

        // Verify that the dependent task has not been executed
        assertThat(Files.exists(outfile), is(false));

        // Verify that the attempt is not yet done
        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getDone(), is(false));

        // Create the file that the workflow is waiting for
        String content = "hello world";
        s3.putObject(bucket, key, new StringInputStream(content), new ObjectMetadata());

        // Expect the attempt to finish and the dependent task to be executed
        expect(Duration.ofMinutes(2), attemptSuccess(server.endpoint(), attemptId));
        assertThat(Files.exists(outfile), is(true));

        JsonNode objectMetadata = MAPPER.readTree(Files.readAllBytes(outfile));
        int contentLength = objectMetadata.get("metadata").get("Content-Length").asInt();
        assertThat(contentLength, is(content.length()));
    }

    @Test
    public void testTimeout()
            throws Exception
    {
        String key = UUID.randomUUID().toString();

        Path outfile = folder.newFolder().toPath().resolve("out");

        createProject(projectDir);
        addWorkflow(projectDir, "acceptance/s3/s3_wait_timeout.dig");

        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        // Configure AWS credentials
        client.setProjectSecret(projectId, "aws.s3.access_key_id", TEST_S3_ACCESS_KEY_ID);
        client.setProjectSecret(projectId, "aws.s3.secret_access_key", TEST_S3_SECRET_ACCESS_KEY);
        client.setProjectSecret(projectId, "aws.s3.endpoint", TEST_S3_ENDPOINT);

        // Start workflow
        String projectName = projectDir.getFileName().toString();
        Id attemptId = startWorkflow(server.endpoint(), projectName, "s3_wait_timeout", ImmutableMap.of(
                "path", bucket + "/" + key,
                "outfile", outfile.toString()
        ));

        // Wait for s3 polling finish because of timeout
        expect(Duration.ofSeconds(30), () -> {
            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            return attempt.getDone();
        });

        // Verify that the attempt is done and failed
        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getDone(), is(true));
        assertThat(attempt.getSuccess(), is(false));
        assertThat(attempt.getFinishedAt().isPresent(), is(true));
    }

    @Test
    public void testContinueOnTimeout()
            throws Exception
    {
        String key = UUID.randomUUID().toString();

        Path outfile = folder.newFolder().toPath().resolve("out");

        createProject(projectDir);
        addWorkflow(projectDir, "acceptance/s3/s3_wait_continue_on_timeout.dig");

        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        // Configure AWS credentials
        client.setProjectSecret(projectId, "aws.s3.access_key_id", TEST_S3_ACCESS_KEY_ID);
        client.setProjectSecret(projectId, "aws.s3.secret_access_key", TEST_S3_SECRET_ACCESS_KEY);
        client.setProjectSecret(projectId, "aws.s3.endpoint", TEST_S3_ENDPOINT);

        // Start workflow
        String projectName = projectDir.getFileName().toString();
        Id attemptId = startWorkflow(server.endpoint(), projectName, "s3_wait_continue_on_timeout", ImmutableMap.of(
                "path", bucket + "/" + key,
                "outfile", outfile.toString()
        ));

        // Wait for s3 polling finish because of timeout
        expect(Duration.ofSeconds(30), () -> {
            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            return attempt.getDone();
        });

        // Verify that the attempt is done and failed
        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getDone(), is(true));
        assertThat(attempt.getSuccess(), is(true));
        assertThat(attempt.getFinishedAt().isPresent(), is(true));

        //Verify outfile
        String outfileText = new String(Files.readAllBytes(outfile), UTF_8);
        assertThat(outfileText.contains("Finished task +wait"), is(true));
        assertThat(outfileText.contains("Read s3 variable"), is(true));

    }

}
