package acceptance;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.fakeHome;
import static acceptance.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class InitPushStartIT
{
    private static final Pattern START_ATTEMPT_ID_PATTERN = Pattern.compile("\\s*id:\\s*(\\d+)\\s*");
    private static final Pattern ATTEMPTS_ATTEMPT_ID_PATTERN = Pattern.compile("\\s*attempt id:\\s*(\\d+)\\s*");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void initPushStart()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.yml", projectDir.resolve("foobar.yml"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "foobar",
                "-c", config.toString(),
                "-f", projectDir.resolve("digdag.yml").toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Verify that the project is there
        RestProject project = client.getProject("foobar");

        assertThat(project.getName(), is("foobar"));
        assertThat(project.getRevision(), is("4711"));
        long now = Instant.now().toEpochMilli();
        long error = MINUTES.toMillis(1);
        assertThat(project.getCreatedAt().toEpochMilli(), is(both(
                greaterThan(now - error))
                .and(lessThan(now + error))));

        // Start the workflow
        long attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            Matcher startAttemptIdMatcher = START_ATTEMPT_ID_PATTERN.matcher(startStatus.outUtf8());
            assertThat(startAttemptIdMatcher.find(), is(true));
            attemptId = Long.parseLong(startAttemptIdMatcher.group(1));
        }

        // Verify that the workflow is started
        {
            List<RestSessionAttempt> attempts = client.getSessionAttempts(true, Optional.absent());
            assertThat(attempts.size(), is(1));
            RestSessionAttempt attempt = attempts.get(0);
            assertThat(attempt.getProject().getName(), is("foobar"));
            assertThat(attempt.getId(), is(attemptId));
        }
        {
            RestSessionAttempt attemptById = client.getSessionAttempt(attemptId);
            assertThat(attemptById.getProject().getName(), is("foobar"));
            assertThat(attemptById.getId(), is(attemptId));
        }
        {
            List<CommandStatus> attemptsStatuses = ImmutableList.of(
                    // By attempt id
                    main("attempts",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            String.valueOf(attemptId)),
                    // By project name
                    main("attempts",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            "foobar"),
                    // By project and workflow name
                    main("attempts",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            "foobar", "foobar"));
            for (CommandStatus attemptsStatus : attemptsStatuses) {
                assertThat(attemptsStatus.code(), is(0));
                Matcher attemptsAttemptIdMatcher = ATTEMPTS_ATTEMPT_ID_PATTERN.matcher(attemptsStatus.outUtf8());
                assertThat(attemptsAttemptIdMatcher.find(), is(true));
                assertThat(Long.parseLong(attemptsAttemptIdMatcher.group(1)), is(attemptId));
            }
        }

        // Wait for the attempt to complete
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

        // Verify that the attempt success is reflected in the cli
        {
            CommandStatus attemptsStatus = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: success"));
        }
    }
}
