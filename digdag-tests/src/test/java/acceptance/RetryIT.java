package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class RetryIT
{
    private static final Pattern START_ATTEMPT_ID_PATTERN = Pattern.compile("\\s*id:\\s*(\\d+)\\s*");

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

        copyResource("acceptance/retry/digdag.yml", projectDir.resolve("digdag.yml"));
    }

    @Test
    public void testRetry()
            throws Exception
    {
        DigdagClient client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        copyResource("acceptance/retry/fail.yml", projectDir.resolve("foobar.yml"));

        // Push the project
        {
            CommandStatus pushStatus = main("push",
                    "foobar",
                    "-c", config.toString(),
                    "-f", projectDir.resolve("digdag.yml").toString(),
                    "-e", server.endpoint(),
                    "-r", "1");
            assertThat(pushStatus.code(), is(0));
        }

        // Start the workflow
        long originalAttemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            Matcher startAttemptIdMatcher = START_ATTEMPT_ID_PATTERN.matcher(startStatus.outUtf8());
            assertThat(startAttemptIdMatcher.find(), is(true));
            originalAttemptId = Long.parseLong(startAttemptIdMatcher.group(1));
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
            CommandStatus retryStatus = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--name", "retry-not-fixed",
                    "--latest-revision",
                    "--all",
                    String.valueOf(originalAttemptId));
            assertThat(retryStatus.code(), is(0));
            Matcher attemptIdMatcher = START_ATTEMPT_ID_PATTERN.matcher(retryStatus.outUtf8());
            assertThat(attemptIdMatcher.find(), is(true));
            retryFailAttemptId = Long.parseLong(attemptIdMatcher.group(1));
            assertThat(retryFailAttemptId, is(not(originalAttemptId)));
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
            copyResource("acceptance/retry/succeed.yml", projectDir.resolve("foobar.yml"));
            CommandStatus pushStatus = main("push",
                    "foobar",
                    "-c", config.toString(),
                    "-f", projectDir.resolve("digdag.yml").toString(),
                    "-e", server.endpoint(),
                    "-r", "2");
            assertThat(pushStatus.code(), is(0));
        }

        // Start a retry of the fixed workflow
        {
            CommandStatus retryStatus = main("retry",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--name", "retry-fixed",
                    "--latest-revision",
                    "--all",
                    String.valueOf(originalAttemptId));
            assertThat(retryStatus.code(), is(0));
            Matcher attemptIdMatcher = START_ATTEMPT_ID_PATTERN.matcher(retryStatus.outUtf8());
            assertThat(attemptIdMatcher.find(), is(true));
            retryFailAttemptId = Long.parseLong(attemptIdMatcher.group(1));
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
