package acceptance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getSessionId;
import static utils.TestUtils.main;

public class InitPushStartIT
{
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

        copyResource("acceptance/basic.dig", projectDir.resolve("dummy.dig"));

        // Push should work without -r
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        copyResource("acceptance/basic.dig", projectDir.resolve("foobar.dig"));

        // Push the project and the later push should be the latest revision
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

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
        long sessionId;
        long attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            sessionId = getSessionId(startStatus);
            attemptId = getAttemptId(startStatus);
        }

        // Verify that the workflow is started
        RestSession session;
        {
            List<RestSession> sessions = client.getSessions();
            assertThat(sessions.size(), is(1));
            session = sessions.get(0);
            assertThat(session.getProject().getName(), is("foobar"));
            assertThat(session.getId(), is(sessionId));
            assertThat(session.getLastAttempt().isPresent(), is(true));
            assertThat(session.getLastAttempt().get().getId(), is(attemptId));
        }

        // Fetch session using client
        {
            RestSession sessionById = client.getSession(sessionId);
            assertThat(sessionById, is(session));
            List<RestSession> sessionsByProject = client.getSessions(project.getId());
            assertThat(sessionsByProject, contains(session));
            List<RestSession> sessionsByWorkflowName = client.getSessions(project.getId(), "foobar");
            assertThat(sessionsByWorkflowName, contains(session));
        }

        // Fetch attempt using client
        {
            List<RestSessionAttempt> attempts = client.getSessionAttempts(Optional.absent());
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

        // Fetch session using cli
        {
            // Listing all
            {
                List<RestSession> ss = sessions();
                assertThat(ss.size(), is(1));
                assertThat(ss.get(0).getId(), is(sessionId));
                assertThat(ss.get(0).getLastAttempt().get().getId(), is(attemptId));
            }

            // By project name
            {
                List<RestSession> ss = sessions(project.getName());
                assertThat(ss.size(), is(1));
                assertThat(ss.get(0).getId(), is(sessionId));
                assertThat(ss.get(0).getLastAttempt().get().getId(), is(attemptId));
            }

            // By project and workflow name
            {
                List<RestSession> ss = sessions(project.getName(), "foobar");
                assertThat(ss.size(), is(1));
                assertThat(ss.get(0).getId(), is(sessionId));
                assertThat(ss.get(0).getLastAttempt().get().getId(), is(attemptId));
            }

            // By session id
            {
                CommandStatus status = main("session",
                        "--json",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        String.valueOf(sessionId));
                assertThat(status.code(), is(0));
                assertThat(status.outJson(RestSession.class), is(session));
            }
        }

        // Fetch attempt using cli
        {
            // By attempt id
            {
                CommandStatus status = main("attempt",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        String.valueOf(attemptId));
                assertThat(status.code(), is(0));
                assertThat(getSessionId(status), is(sessionId));
                assertThat(getAttemptId(status), is(attemptId));
            }

            // By session id
            {
                List<RestSessionAttempt> attempts = attempts(String.valueOf(sessionId));
                assertThat(attempts.size(), is(1));
                RestSessionAttempt attempt = attempts.get(0);
                assertThat(attempt.getSessionId(), is(sessionId));
                assertThat(attempt.getId(), is(attemptId));
            }
        }

        // Wait for the attempt to complete
        expect(Duration.ofSeconds(30), attemptSuccess(server.endpoint(), attemptId));

        // Verify that the success is reflected in the cli
        {
            // For the attempt
            {
                CommandStatus attemptsStatus = main("attempt",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        String.valueOf(attemptId));
                assertThat(attemptsStatus.outUtf8(), containsString("status: success"));
            }

            // For the attempt by session id
            {
                List<RestSessionAttempt> attempts = attempts(String.valueOf(sessionId));
                assertThat(attempts.size(), is(1));
                RestSessionAttempt attempt = attempts.get(0);
                assertThat(attempt.getSessionId(), is(sessionId));
                assertThat(attempt.getId(), is(attemptId));
                assertThat(attempt.getSuccess(), is(true));
            }

            // For the session
            {
                CommandStatus attemptsStatus = main("sessions",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        String.valueOf(sessionId));
                assertThat(attemptsStatus.outUtf8(), containsString("status: success"));
            }
        }
    }

    @Test
    public void startFailsWithConflictedSessionTime()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // Start the workflow with session_time = 2016-01-01
        long sessionId;
        long attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "2016-01-01");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
            sessionId = getSessionId(startStatus);
            attemptId = getAttemptId(startStatus);
        }

        // Try to start the workflow with the same session_time
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "foobar", "foobar",
                "--session", "2016-01-01");

        // should fail with a hint message
        assertThat(startStatus.errUtf8(), startStatus.code(), is(1));
        assertThat(startStatus.errUtf8(), containsString("A session for the requested session_time already exists (session_id=" + sessionId + ", attempt_id=" + attemptId + ", session_time=2016-01-01T00:00Z)"));
        assertThat(startStatus.errUtf8(), containsString("hint: use `digdag retry " + attemptId + " --latest-revision` command to run the session again for the same session_time"));
    }

    private List<RestSession> sessions(String... args)
            throws IOException
    {
        CommandStatus status = main(FluentIterable.from(asList(
                "sessions",
                "--json",
                "-c", config.toString(),
                "-e", server.endpoint()))
                .append(args)
                .toList());
        assertThat(status.code(), is(0));
        return status.outJson(new TypeReference<List<RestSession>>() {});
    }

    private List<RestSessionAttempt> attempts(String... args)
            throws IOException
    {
        CommandStatus status = main(FluentIterable.from(asList(
                "attempts",
                "--json",
                "-c", config.toString(),
                "-e", server.endpoint()))
                .append(args)
                .toList());
        assertThat(status.code(), is(0));
        return status.outJson(new TypeReference<List<RestSessionAttempt>>() {});
    }
}
