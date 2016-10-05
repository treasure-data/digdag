package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.ServiceUnavailableException;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class ServerGracefulShutdownIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .inProcess(false)
            .build();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(server.hasUnixProcess(), is(true));

        projectDir = root().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    private Id startSleepTask()
        throws Exception
    {
        Id attemptId;
        {
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/server_graceful_shutdown/sleep.dig", projectDir.resolve("sleep.dig"));

            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "server_graceful_shutdown",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-p", "outdir=" + root());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "server_graceful_shutdown", "sleep",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the task to start
        TestUtils.expect(Duration.ofMinutes(5), () -> {
            RestTask checkerTask = client.getTasks(attemptId).getTasks()
                .stream()
                .filter(it -> it.getFullName().endsWith("+start_checker"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("+start_checker task doesn't exist"));

            if (checkerTask.getState().equals("success")) {
                return true;
            }

            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                return true;
            }

            return false;
        });

        return attemptId;
    }

    @Test
    public void gracefulShutdown()
            throws Exception
    {
        Id attemptId = startSleepTask();

        server.terminateProcess();

        // server started termination but it should be alive at most 5 seconds.

        int aliveSeconds = 0;
        while (true) {
            try {
                client.getSessionAttempt(attemptId);
                aliveSeconds++;
            }
            catch (Exception ex) {
                // if REST API fails, the cause should be 503 Service Unavailable or
                // connection refused.
                if (ex instanceof ProcessingException) {
                    assertThat(ex.getCause(), instanceOf(ConnectException.class));
                    break;
                }
                else {
                    assertThat(ex, instanceOf(ServiceUnavailableException.class));
                    break;
                }
            }

            if (aliveSeconds > Duration.ofMinutes(5).getSeconds()) {
                throw new IllegalStateException("Server doesn't shutdown");
            }

            Thread.sleep(1000);
        }

        // all running tasks should be done
        assertThat(Files.exists(root().resolve("done.out")), is(true));

        // but waiting tasks should not start
        assertThat(Files.exists(root().resolve("after_sleep.out")), is(false));

        // REST API should be alive for a while
        assertThat(aliveSeconds, greaterThan(3));

        assertThat(server.outUtf8(), containsString("Waiting for completion of 2 running tasks..."));
        assertThat(server.outUtf8(), containsString("Closing HTTP listening sockets"));

        TestUtils.expect(Duration.ofMinutes(5), () -> !server.isProcessAlive());

        assertThat(server.outUtf8(), containsString("Shutting down HTTP worker threads"));
        assertThat(server.outUtf8(), containsString("Shutting down system"));
        assertThat(server.outUtf8(), containsString("Shutdown completed"));
    }
}
