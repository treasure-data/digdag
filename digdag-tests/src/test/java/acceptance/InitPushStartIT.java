package acceptance;

import com.google.common.base.Optional;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
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
        assertThat((double) project.getCreatedAt().toEpochMilli(), is(closeTo(Instant.now().toEpochMilli(), MINUTES.toMillis(1))));

        Instant startTime = Instant.now().truncatedTo(SECONDS);

        // Start the workflow
        Id sessionId;
        Id attemptId;
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
        List<RestSession> sessions = client.getSessions().getSessions();
        assertThat(sessions.size(), is(1));
        RestSession session = sessions.get(0);
        assertThat(session.getProject().getName(), is("foobar"));
        assertThat(session.getId(), is(sessionId));
        assertThat(session.getLastAttempt().isPresent(), is(true));
        assertThat(session.getLastAttempt().get().getId(), is(attemptId));

        // Fetch session using client
        {
            RestSession sessionById = client.getSession(sessionId);
            assertThat(sessionById, is(session));
            List<RestSession> sessionsByProject = client.getSessions(project.getId()).getSessions();
            assertThat(sessionsByProject, contains(session));
            List<RestSession> sessionsByWorkflowName = client.getSessions(project.getId(), "foobar").getSessions();
            assertThat(sessionsByWorkflowName, contains(session));
        }

        // Fetch attempt using client
        {
            List<RestSessionAttempt> attempts = client.getSessionAttempts(Optional.absent(), Optional.absent()).getAttempts();
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
                CommandStatus status = main("sessions",
                        "-c", config.toString(),
                        "-e", server.endpoint());
                assertThat(status.code(), is(0));
                assertThat(getSessionId(status), is(sessionId));
                assertThat(getAttemptId(status), is(attemptId));
            }

            // By project name
            {
                CommandStatus status = main("sessions",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        project.getName());
                assertThat(status.code(), is(0));
                assertThat(getSessionId(status), is(sessionId));
                assertThat(getAttemptId(status), is(attemptId));
            }

            // By project and workflow name
            {
                CommandStatus status = main("sessions",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        project.getName(),
                        "foobar");
                assertThat(status.code(), is(0));
                assertThat(getSessionId(status), is(sessionId));
                assertThat(getAttemptId(status), is(attemptId));
            }

            // By session id
            {
                CommandStatus status = main("sessions",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        String.valueOf(sessionId));
                assertThat(status.code(), is(0));
                assertThat(getSessionId(status), is(sessionId));
                assertThat(getAttemptId(status), is(attemptId));
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
                CommandStatus status = main("attempts",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        String.valueOf(sessionId));
                assertThat(status.code(), is(0));
                assertThat(getSessionId(status), is(sessionId));
                assertThat(getAttemptId(status), is(attemptId));
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

            // Verify that the success is reflected to the REST API
            assertThat(attempt.getSuccess(), is(true));
            assertThat(attempt.getFinishedAt().isPresent(), is(true));
            {
                RestSession done = client.getSession(sessionId);
                assertThat(done.getLastAttempt().get().getDone(), is(true));
                assertThat(done.getLastAttempt().get().getSuccess(), is(true));
                assertThat(done.getLastAttempt().get().getFinishedAt().isPresent(), is(true));
            }
        }

        // Fetch tasks and check task execution times
        {
            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);

            // Client
            Map<String, RestTask> tasks = client.getTasks(attemptId)
                    .getTasks().stream().collect(Collectors.toMap(RestTask::getFullName, t -> t));
            assertThat(tasks, hasKey("+foobar"));
            assertThat(tasks, hasKey("+foobar+foo"));
            assertThat(tasks, hasKey("+foobar+bar"));
            Instant fooStart = tasks.get("+foobar+foo").getStartedAt().get();
            Instant fooEnd = tasks.get("+foobar+foo").getUpdatedAt();
            Instant barStart = tasks.get("+foobar+bar").getStartedAt().get();
            Instant barEnd = tasks.get("+foobar+bar").getUpdatedAt();
            assertThat(fooStart, is(both(greaterThanOrEqualTo(startTime)).and(lessThanOrEqualTo(fooEnd))));
            assertThat(fooEnd, is(both(greaterThanOrEqualTo(fooStart)).and(lessThanOrEqualTo(barStart))));
            assertThat(barStart, is(both(greaterThanOrEqualTo(fooEnd)).and(lessThanOrEqualTo(barEnd))));
            assertThat(barEnd, is(both(greaterThanOrEqualTo(barStart)).and(lessThanOrEqualTo(attempt.getFinishedAt().get()))));

            // cli
            CommandStatus tasksStatus = main("tasks",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            String out = tasksStatus.outUtf8();
            assertThat(StringUtils.countMatches(out, "state: success"), is(3));
            assertThat(out, Matchers.containsString("started: " + TimeUtil.formatTime(fooStart)));
            assertThat(out, Matchers.containsString("updated: " + TimeUtil.formatTime(fooEnd)));
            assertThat(out, Matchers.containsString("started: " + TimeUtil.formatTime(barStart)));
            assertThat(out, Matchers.containsString("updated: " + TimeUtil.formatTime(barEnd)));
        }

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
        Id sessionId;
        Id attemptId;
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
        assertThat(startStatus.errUtf8(), containsString("hint: use `digdag retry " + attemptId));
    }
}
