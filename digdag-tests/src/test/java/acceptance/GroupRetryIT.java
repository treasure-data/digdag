package acceptance;

import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;

public class GroupRetryIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;

    @Before
    public void setUp()
            throws Exception
    {
        config = folder.newFile().toPath();

        projectDir = folder.getRoot().toPath().resolve("group_retry");
        Files.createDirectory(projectDir);
    }

    @Test
    public void testGroupRetry()
            throws Exception
    {
        DigdagClient client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        // Push the project
        pushRevision("acceptance/group_retry/retry-1.dig", "test");

        // Start the workflow
        Id originalAttemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "group_retry", "test",
                    "--session", "now");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
            originalAttemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to fail
        assertThat(joinAttempt(client, originalAttemptId).getSuccess(), is(false));

        assertOutputExists(originalAttemptId + "1-1", true);
        assertOutputExists(originalAttemptId + "1-2a", true);
        assertOutputExists(originalAttemptId + "1-2b", true);
        assertOutputExists(originalAttemptId + "1-2c", false);

        // Push a new revision
        pushRevision("acceptance/group_retry/retry-2.dig", "test");

        // Retry without updating the revision: --keep-revision
        Id retry1;
        {
            CommandStatus retryStatus = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--keep-revision",
                    "--all",
                    String.valueOf(originalAttemptId));
            assertThat(retryStatus.errUtf8(), retryStatus.code(), is(0));
            retry1 = getAttemptId(retryStatus);
        }

        // Wait for the attempt to fail again
        assertThat(joinAttempt(client, retry1).getSuccess(), is(false));

        assertOutputExists(retry1 + "1-1", true);
        assertOutputExists(retry1 + "1-2a", true);
        assertOutputExists(retry1 + "1-2b", true);
        assertOutputExists(retry1 + "1-2c", false);

        // Retry with the latest fixed revision & resume failed
        Id retry2;
        {
            CommandStatus retryStatus = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--latest-revision",
                    "--resume",
                    String.valueOf(originalAttemptId));
            assertThat(retryStatus.errUtf8(), retryStatus.code(), is(0));
            retry2 = getAttemptId(retryStatus);
        }

        // Wait for the attempt to success
        assertThat(joinAttempt(client, retry2).getSuccess(), is(true));

        assertOutputExists(retry2 + "2-1", false);  // skipped
        assertOutputExists(retry2 + "2-2a", true);  // no-skipped
        assertOutputExists(retry2 + "2-2b", true);  // no-skipped
        assertOutputExists(retry2 + "2-2c", true);

        // Retry with the latest fixed revision & resume all
        Id retry3;
        {
            CommandStatus retryStatus = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--latest-revision",
                    "--all",
                    String.valueOf(originalAttemptId));
            assertThat(retryStatus.code(), is(0));
            retry3 = getAttemptId(retryStatus);
        }

        // Wait for the attempt to success
        assertThat(joinAttempt(client, retry3).getSuccess(), is(true));

        assertOutputExists(retry3 + "2-1", true);
        assertOutputExists(retry3 + "2-2a", true);
        assertOutputExists(retry3 + "2-2b", true);
        assertOutputExists(retry3 + "2-2c", true);

        // Push another new revision
        pushRevision("acceptance/group_retry/retry-3.dig", "test");

        // Retry with the latest fixed revision & resume from
        Id retry4;
        {
            CommandStatus retryStatus = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--latest-revision",
                    "--resume-from", "+step2+a",
                    String.valueOf(originalAttemptId));
            assertThat(retryStatus.errUtf8(), retryStatus.code(), is(0));
            retry4 = getAttemptId(retryStatus);
        }

        // Wait for the attempt to success
        assertThat(joinAttempt(client, retry4).getSuccess(), is(true));

        assertOutputExists(retry4 + "3-1", false); // skipped
        assertOutputExists(retry4 + "3-2a", true);
        assertOutputExists(retry4 + "3-2b", true);
        assertOutputExists(retry4 + "3-2c", true);
    }

    private void pushRevision(String resourceName, String workflowName)
            throws IOException
    {
        Files.write(projectDir.resolve(workflowName + ".dig"), asList(Resources.toString(
                Resources.getResource(resourceName), UTF_8)
                .replace("${outdir}", root().toString())));

        CommandStatus pushStatus = main("push",
                "group_retry",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "-e", server.endpoint());
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
    }

    private RestSessionAttempt joinAttempt(DigdagClient client, Id attemptId)
            throws InterruptedException
    {
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 30; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        return attempt;
    }

    private void assertOutputExists(String name, boolean exists)
    {
        assertThat(Files.exists(root().resolve(name + ".out")), is(exists));
    }

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }
}
