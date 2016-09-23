package acceptance;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import org.apache.commons.lang3.RandomUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

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
    private static final String FAKE_S3_ENDPOINT = System.getenv("FAKE_S3_ENDPOINT");

    private static final ObjectMapper MAPPER = DigdagClient.objectMapper();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "digdag.secret-encryption-key = " + Base64.getEncoder().encodeToString(RandomUtils.nextBytes(16)))
            .build();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(FAKE_S3_ENDPOINT, not(isEmptyOrNullString()));

        projectDir = folder.getRoot().toPath().resolve("foobar");

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void testRun()
            throws Exception
    {
        AWSCredentials credentials = new BasicAWSCredentials("test-access-key", "test-secret-key");
        AmazonS3Client s3Client = new AmazonS3Client(credentials);
        s3Client.setEndpoint(FAKE_S3_ENDPOINT);

        String bucket = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();

        Path outfile = folder.newFolder().toPath().resolve("out");

        createProject(projectDir);
        addWorkflow(projectDir, "acceptance/s3/s3_wait.dig");

        int projectId = TestUtils.pushProject(server.endpoint(), projectDir);

        // Configure AWS credentials
        client.setProjectSecret(projectId, "aws.access-key", "test-access-key");
        client.setProjectSecret(projectId, "aws.secret-key", "test-secret-key");
        client.setProjectSecret(projectId, "aws.endpoint", FAKE_S3_ENDPOINT);

        // Start workflow
        String projectName = projectDir.getFileName().toString();
        long attemptId = startWorkflow(server.endpoint(), projectName, "s3_wait", ImmutableMap.of(
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
        s3Client.putObject(bucket, key, new StringInputStream(content), new ObjectMetadata());

        // Expect the attempt to finish and the dependent task to be executed
        expect(Duration.ofSeconds(30), attemptSuccess(server.endpoint(), attemptId));
        assertThat(Files.exists(outfile), is(true));

        JsonNode objectMetadata = MAPPER.readTree(Files.readAllBytes(outfile));
        int contentLength = objectMetadata.get("metadata").get("Content-Length").asInt();
        assertThat(contentLength, is(content.length()));
    }
}
