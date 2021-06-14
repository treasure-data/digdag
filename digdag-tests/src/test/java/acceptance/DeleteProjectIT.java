package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.time.Duration;

import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getSessionId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class DeleteProjectIT
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
    public void deleteProject()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("foobar.dig"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "foobar",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Run the workflow once before deleting the project
        Id attemptId;
        Id sessionId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "now");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
            sessionId = getSessionId(startStatus);
            attemptId = getAttemptId(startStatus);

        }
        expect(Duration.ofSeconds(30), attemptSuccess(server.endpoint(), attemptId));

        CommandStatus attempt = main("attempt",
                "-c", config.toString(),
                "-e", server.endpoint(),
                attemptId.toString());
        CommandStatus attempts = main("attempts",
                "-c", config.toString(),
                "-e", server.endpoint(),
                sessionId.toString());
        CommandStatus logs = main("logs",
                "-c", config.toString(),
                "-e", server.endpoint(),
                attemptId.toString());

        // Delete the project
        {
            CommandStatus deleteStatus = main("delete",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar",
                    "--force");
            assertThat(deleteStatus.errUtf8(), deleteStatus.code(), is(0));
        }

        // Starting a workflow should fail
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar",
                    "--session", "now");
            assertThat(startStatus.code(), is(not(0)));
        }

        // The project should not be listed
        {
            CommandStatus workflows = main("workflows",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(workflows.errUtf8(), workflows.code(), is(0));
            assertThat(workflows.outUtf8(), not(containsString("foobar")));
        }
        {
            CommandStatus workflows = main("workflows",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar");
            assertThat(workflows.errUtf8(), workflows.code(), is(not(0)));
            assertThat(workflows.errUtf8(), containsString("project not found: foobar"));
        }
        {
            CommandStatus workflows = main("workflows",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "foobar");
            assertThat(workflows.errUtf8(), workflows.code(), is(not(0)));
            assertThat(workflows.errUtf8(), containsString("project not found: foobar"));
        }

        // The session, attempt and logs should still be accessible
        {
            CommandStatus status = main("attempt",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(status.outUtf8(), is(attempt.outUtf8()));
        }
        {
            CommandStatus status = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(sessionId));
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(status.outUtf8(), is(attempts.outUtf8()));
        }
        {
            CommandStatus status = main("logs",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(status.outUtf8(), is(logs.outUtf8()));
        }

    }
}

