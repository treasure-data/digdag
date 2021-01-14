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

    @Test
    public void testRun()
            throws Exception
    {
        Duration baselineDuration;
        {
            copyResource("acceptance/wait/nowait.dig", root().resolve("nowait.dig"));
            ExecResult result = runAndMonitorDuration(() ->
                    main("run", "-o", root().toString(), "--project", root().toString(), "nowait.dig"));
            CommandStatus status = result.commandStatus;
            assertThat(status.errUtf8(), status.code(), is(0));
            baselineDuration = result.duration;
        }

        {
            copyResource("acceptance/wait/wait_10s.dig", root().resolve("wait_10s.dig"));
            ExecResult result = runAndMonitorDuration(() ->
                    main("run", "-o", root().toString(), "--project", root().toString(), "wait_10s.dig"));
            CommandStatus status = result.commandStatus;
            assertThat(status.errUtf8(), status.code(), is(0));
            assertThat(result.duration, greaterThan(baselineDuration));
            assertThat(result.duration, lessThan(
                    // Actual wait duration can be longer than the specified 10 seconds for some reason
                    baselineDuration.plusSeconds((long) (10 * 1.5))));
        }
    }
}
