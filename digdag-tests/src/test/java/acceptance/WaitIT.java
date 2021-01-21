package acceptance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class WaitIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private static class ExecResult
    {
        CommandStatus commandStatus;
        Duration duration;

        public ExecResult(CommandStatus commandStatus, Duration duration)
        {
            this.commandStatus = commandStatus;
            this.duration = duration;
        }
    }

    private ExecResult runAndMonitorDuration(Supplier<CommandStatus> task)
    {
        Instant start = Instant.now();
        CommandStatus commandStatus = task.get();
        Duration duration = Duration.between(start, Instant.now());
        return new ExecResult(commandStatus, duration);
    }

    private void testWorkflow(String workflowName, int expectedDuration)
            throws Exception
    {
        String nowaitResourcePath = "acceptance/wait/nowait.dig";
        String targetResourcePath = "acceptance/wait/" + workflowName;

        Duration baselineDuration;
        {
            copyResource(nowaitResourcePath, root().resolve("wait.dig"));
            ExecResult result = runAndMonitorDuration(() ->
                    main("run", "-o", root().toString(), "--project", root().toString(), "wait.dig"));
            CommandStatus status = result.commandStatus;
            assertThat(status.errUtf8(), status.code(), is(0));
            baselineDuration = result.duration;
        }

        {
            copyResource(targetResourcePath, root().resolve("wait.dig"));
            ExecResult result = runAndMonitorDuration(() ->
                    main("run", "-o", root().toString(), "--project", root().toString(), "wait.dig"));
            CommandStatus status = result.commandStatus;
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(result.duration, greaterThan(baselineDuration));
            assertThat(result.duration, lessThan(
                    // Actual wait duration can be longer than the specified 10 seconds for some reason
                    baselineDuration.plusSeconds(expectedDuration * 3)));
        }
    }

    @Test
    public void testSimpleVersion()
            throws Exception
    {
        testWorkflow("wait.dig", 10);
    }

    @Test
    public void testBlockingMode()
            throws Exception
    {
        testWorkflow("wait_blocking.dig", 10);
    }

    @Test
    public void testNonBlockingMode()
            throws Exception
    {
        testWorkflow("wait_nonblocking.dig", 10);
    }

    @Test
    public void testPollInterval()
            throws Exception
    {
        testWorkflow("wait_poll_interval.dig", 10);
    }

    @Test
    public void testInvalidConfig()
            throws Exception
    {
        String targetResourcePath = "acceptance/wait/wait_invalid_config.dig";

        copyResource(targetResourcePath, root().resolve("wait.dig"));
        ExecResult result = runAndMonitorDuration(() ->
                main("run", "-o", root().toString(), "--project", root().toString(), "wait.dig"));
        CommandStatus status = result.commandStatus;
        // The workflow contains a conflict configuration and it should fail.
        assertThat(status.errUtf8(), status.code(), is(1));
    }
}
