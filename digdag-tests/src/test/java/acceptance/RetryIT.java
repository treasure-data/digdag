package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static acceptance.CommandStatus.success;
import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.getStartAttemptId;
import static acceptance.TestUtils.main;
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

        projectDir = folder.getRoot().toPath().resolve("foobar");
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

        copyResource("acceptance/retry/fail.dig", projectDir.resolve("foobar.dig"));

        // Push the project
        {
            CommandStatus status = main("push",
                    "foobar",
                    "-c", config.toString(),
                    "--project", projectDir.toString(),
                    "-e", server.endpoint(),
                    "-r", "1");
            assertThat(status, is(success()));
        }

        // Start the workflow
        long originalAttemptId;
        {
            CommandStatus status = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "now");
            assertThat(status, is(success()));
            originalAttemptId = getStartAttemptId(status);
        }

        // Wait for the attempt to fail
        {
            RestSessionAttempt attempt = null;
            for (int i = 0; i < 30; i++) {
                attempt = client.getSessionAttempt(originalAttemptId);
                if (attempt.getDone()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertThat(attempt.getSuccess(), is(false));
        }

        // Start a retry of the failing workflow
        long retryFailAttemptId;
        {
            CommandStatus status = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--name", "retry-not-fixed",
                    "--latest-revision",
                    "--all",
                    String.valueOf(originalAttemptId));
            assertThat(status, is(success()));
            retryFailAttemptId = getStartAttemptId(status);
        }

        // Wait for the retry to fail as well
        {
            RestSessionAttempt attempt = null;
            for (int i = 0; i < 30; i++) {
                attempt = client.getSessionAttempt(retryFailAttemptId);
                if (attempt.getDone()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertThat(attempt.getSuccess(), is(false));
        }

        // "Fix" the workflow
        {
            copyResource("acceptance/retry/succeed.dig", projectDir.resolve("foobar.dig"));
            CommandStatus status = main("push",
                    "foobar",
                    "-c", config.toString(),
                    "--project", projectDir.toString(),
                    "-e", server.endpoint(),
                    "-r", "2");
            assertThat(status, is(success()));
        }

        // Start a retry of the fixed workflow
        {
            CommandStatus status = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--name", "retry-fixed",
                    "--latest-revision",
                    "--all",
                    String.valueOf(originalAttemptId));
            assertThat(status, is(success()));
            retryFailAttemptId = getStartAttemptId(status);
            assertThat(retryFailAttemptId, is(not(originalAttemptId)));
        }

        // Wait for the retry of the fixed workflow to succeed
        {
            RestSessionAttempt attempt = null;
            for (int i = 0; i < 30; i++) {
                attempt = client.getSessionAttempt(retryFailAttemptId);
                if (attempt.getDone()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertThat(attempt.getSuccess(), is(true));
        }
    }
}
