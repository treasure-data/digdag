package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class RetryIT
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

        projectDir = folder.getRoot().toPath().resolve("retry");
        Files.createDirectory(projectDir);
    }

    @Test
    public void testRetry()
            throws Exception
    {
        DigdagClient client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        // Push the project
        pushRevision("acceptance/retry/retry-1.dig", "retry");

        // Start the workflow
        long originalAttemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "retry", "retry",
                    "--session", "now");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
            originalAttemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to fail
        assertThat(joinAttempt(client, originalAttemptId).getSuccess(), is(false));

        assertOutputExists("1-1", true);
        assertOutputExists("1-2a", true);

        // Push a new revision
        pushRevision("acceptance/retry/retry-2.dig", "retry");

        // Retry without updating the revision: --keep-revision
        long retry1;
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

        assertOutputExists("2-1", false);
        assertOutputExists("2-2a", false);
        assertOutputExists("2-2b", false);

        // Retry with the latest fixed revision & resume failed
        long retry2;
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

        assertOutputExists("2-1", false);  // skipped
        assertOutputExists("2-2a", false);  // skipped
        assertOutputExists("2-2b", true);

        // Retry with the latest fixed revision & resume all
        long retry3;
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

        assertOutputExists("2-1", true);
        assertOutputExists("2-2a", true);
        assertOutputExists("2-2b", true);

        // Push another new revision
        pushRevision("acceptance/retry/retry-3.dig", "retry");

        // Retry with the latest fixed revision & resume from
        long retry4;
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

        assertOutputExists("3-1", false);  // skipped
        assertOutputExists("3-2a", true);
        assertOutputExists("3-2b", true);
    }

    private void pushRevision(String resourceName, String workflowName)
            throws IOException
    {
        copyResource(resourceName, projectDir.resolve(workflowName + ".dig"));
        CommandStatus pushStatus = main("push",
                "retry",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "-e", server.endpoint(),
                "-p", "outdir=" + root());
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
    }

    private RestSessionAttempt joinAttempt(DigdagClient client, long attemptId)
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
