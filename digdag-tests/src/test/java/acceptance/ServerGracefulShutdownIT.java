package acceptance;

import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.ServiceUnavailableException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;

public class ServerGracefulShutdownIT
{
    private final Logger logger = LoggerFactory.getLogger(ServerGracefulShutdownIT.class);

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
        //This checks Windows environment and then skip
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

            Files.write(projectDir.resolve("sleep.dig"), asList(Resources.toString(
                    Resources.getResource("acceptance/server_graceful_shutdown/sleep.dig"), UTF_8)
                    .replace("${outdir}", root().toString())));

            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "server_graceful_shutdown",
                    "-c", config.toString(),
                    "-e", server.endpoint());
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
                logger.warn("The attempt itself already finished... Is it expected situation....? attempt={}", attempt);
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
        Instant terminateStartedAt = Instant.now();

        // server started termination but it should be alive at most 5 seconds.

        int aliveCount = 0;
        while (true) {
            Instant loopStartedAt = Instant.now();
            if (loopStartedAt.isAfter(terminateStartedAt.plus(Duration.ofMinutes(10)))) {
                throw new IllegalStateException("Server didn't shutdown within 10 minutes");
            }

            try {
                client.getSessionAttempt(attemptId);
                aliveCount++;
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

            // sleep for 1 second
            long sleepMillis = Duration.between(Instant.now(), loopStartedAt.plusSeconds(1)).toMillis();
            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
        }

        // all running tasks should be done
        assertThat(Files.exists(root().resolve("done.out")), is(true));

        // but waiting tasks should not start
        assertThat(Files.exists(root().resolve("after_sleep.out")), is(false));

        // REST API should be alive for a while
        assertThat(aliveCount, greaterThan(3));

        assertThat(server.outUtf8(), containsString("Waiting for completion of 2 running tasks..."));
        assertThat(server.outUtf8(), containsString("Closing HTTP listening sockets"));

        TestUtils.expect(Duration.ofMinutes(5), () -> !server.isProcessAlive());

        assertThat(server.outUtf8(), containsString("Shutting down HTTP worker threads"));
        assertThat(server.outUtf8(), containsString("Shutting down system"));
        assertThat(server.outUtf8(), containsString("Shutdown completed"));
    }
}
