package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Path;
import java.time.Duration;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptFailure;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushAndStart;

public class AttemptTimeoutIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "executor.attempt_ttl = 5s",
                    "executor.attempt_reaping_interval = 1s"
            )
            .build();

    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.newFolder().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void testAttemptTimeout()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/attempt_timeout/attempt_timeout.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "attempt_timeout");
        expect(Duration.ofSeconds(30), attemptFailure(server.endpoint(), attemptId));

        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        assertThat(attempt.getCancelRequested(), is(true));
    }
}
