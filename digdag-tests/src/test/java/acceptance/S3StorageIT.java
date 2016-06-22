package acceptance;

import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.getAttemptId;
import static acceptance.TestUtils.getSessionId;
import static acceptance.TestUtils.main;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class S3StorageIT
{
    private static final String FAKE_S3_ENDPOINT = System.getenv("FAKE_S3_ENDPOINT");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
    .configuration(
            "archive.type = s3\n" +
            "archive.s3.endpoint = " + (FAKE_S3_ENDPOINT == null ? "" : FAKE_S3_ENDPOINT) + "\n" +
            "archive.s3.credentials.access-key-id = fake-key-id\n" +
            "archive.s3.credentials.secret-access-key = fake-access-key\n" +
            "archive.s3.bucket = fake-s3-" + UUID.randomUUID() + "\n" +
            ""
    )
    .build();

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(FAKE_S3_ENDPOINT, is(notNullValue()));

        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
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
        long sessionId;
        long attemptId;
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

        // Fetch logs
        List<RestLogFileHandle> handles = client.getLogFileHandlesOfAttempt(attemptId);
        assertThat(handles.size(), is(not(0)));

        // log storage is not configurable yet
        //for (RestLogFileHandle handle : handles) {
        //    // S3 log backend should support direct download
        //    assertThat(handle.getDirect().isPresent(), is(true));
        //    assertThat(handle.getDirect().transform(direct -> direct.getUrl()).or(""), startsWith("fake-s3"));
        //}
    }
}
