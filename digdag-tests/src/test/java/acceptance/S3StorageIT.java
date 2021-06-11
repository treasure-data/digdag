package acceptance;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.io.ByteStreams;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getSessionId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3StorageIT
{
    private static final String TEST_S3_ENDPOINT = System.getenv("TEST_S3_ENDPOINT");
    private static final String TEST_S3_ACCESS_KEY_ID = System.getenv().getOrDefault("TEST_S3_ACCESS_KEY_ID", "test");
    private static final String TEST_S3_SECRET_ACCESS_KEY = System.getenv().getOrDefault("TEST_S3_SECRET_ACCESS_KEY", "test");

    private final String archiveBucket = "archive-storage-" + UUID.randomUUID();
    private final String logStorageBucket = "log-storage-" + UUID.randomUUID();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "archive.type = s3",
                    "archive.s3.endpoint = " + TEST_S3_ENDPOINT,
                    "archive.s3.bucket = " + archiveBucket,
                    "archive.s3.credentials.access-key-id = " + TEST_S3_ACCESS_KEY_ID,
                    "archive.s3.credentials.secret-access-key = " + TEST_S3_SECRET_ACCESS_KEY,
                    "log-server.type = s3",
                    "log-server.s3.endpoint = " + TEST_S3_ENDPOINT,
                    "log-server.s3.bucket = " + logStorageBucket,
                    "log-server.s3.direct_download = true",
                    "log-server.s3.path = storage-log-test",
                    "log-server.s3.credentials.access-key-id = " + TEST_S3_ACCESS_KEY_ID,
                    "log-server.s3.credentials.secret-access-key = " + TEST_S3_SECRET_ACCESS_KEY,
                    ""
            )
            .build();

    private Path config;
    private Path projectDir;
    private DigdagClient client;
    private AmazonS3Client s3;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(TEST_S3_ENDPOINT, not(isEmptyOrNullString()));

        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        AWSCredentials credentials = new BasicAWSCredentials(TEST_S3_ACCESS_KEY_ID, TEST_S3_SECRET_ACCESS_KEY);
        s3 = new AmazonS3Client(credentials);
        s3.setEndpoint(TEST_S3_ENDPOINT);

        s3.createBucket(archiveBucket);
        s3.createBucket(logStorageBucket);
    }

    @Test
    public void initPushStartWithS3()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        // Push the workflow
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // Start the workflow
        Id sessionId;
        Id attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "basic",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            sessionId = getSessionId(startStatus);
            attemptId = getAttemptId(startStatus);
        }

        // Verify that the attempt successfully finishes
        {
            RestSessionAttempt attempt = null;
            for (int i = 0; i < 30; i++) {
                attempt = client.getSessionAttempt(attemptId);
                if (attempt.getDone()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertThat(attempt.getSuccess(), is(true));
        }

        // Fetch archive
        RestProject proj = client.getProjects().getProjects().get(0);
        byte[] data;
        try (InputStream in = client.getProjectArchive(proj.getId(), proj.getRevision())) {
            data = ByteStreams.toByteArray(in);
        }
        assertThat(data.length, greaterThan(2));
        assertThat(data[0], is((byte) 0x1f));  // check gzip header
        assertThat(data[1], is((byte) 0x8b));

        // Fetch logs
        List<RestLogFileHandle> handles = client.getLogFileHandlesOfAttempt(attemptId).getFiles();
        assertThat(handles.size(), is(not(0)));

        for (RestLogFileHandle handle : handles) {
            // S3 log backend should support direct download
            assertThat(handle.getDirect().isPresent(), is(true));
            assertThat(handle.getDirect().transform(direct -> direct.getUrl()).or("").contains("log-storage"), is(true));
        }
    }
}
