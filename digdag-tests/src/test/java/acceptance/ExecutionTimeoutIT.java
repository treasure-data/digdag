package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushAndStart;

public class ExecutionTimeoutIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TemporaryDigdagServer server;

    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.newFolder().toPath();
    }

    private void setup(String... configuration)
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .configuration(
                        configuration
                )
//                .inProcess(false)
                .build();

        server.start();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @After
    public void tearDownServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    public void testAttemptTimeout()
            throws Exception
    {
        setup("executor.attempt_ttl = 10s",
              "executor.task_ttl = 1d",
              "executor.ttl_reaping_interval = 1s");

        addWorkflow(projectDir, "acceptance/attempt_timeout/attempt_timeout.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "attempt_timeout");

        // Expect the attempt to get canceled
//        expect(Duration.ofMinutes(1), () -> client.getSessionAttempt(attemptId).getCancelRequested());

        // And then the attempt should be done pretty soon
        expect(Duration.ofMinutes(1), () -> client.getSessionAttempt(attemptId).getDone());

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getDone(), is(true));
        assertThat(attempt.getCancelRequested(), is(true));
        assertThat(attempt.getSuccess(), is(false));
    }

    @Test
    public void testTaskTimeout()
            throws Exception
    {
        setup("executor.attempt_ttl = 1d",
              "executor.task_ttl = 10s",
              "executor.ttl_reaping_interval = 1s");

        addWorkflow(projectDir, "acceptance/attempt_timeout/task_timeout.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "task_timeout");

        // Expect the attempt to get canceled when the task times out
        expect(Duration.ofMinutes(1), () -> client.getSessionAttempt(attemptId).getCancelRequested());

        // TODO: implement termination of blocking tasks
        // TODO: verify that blocking tasks are terminated when the attempt is canceled

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getCancelRequested(), is(true));
    }
}
