package acceptance;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import utils.CommandStatus;

public class ErrorTaskIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void errorTaskRuns()
            throws Exception
    {
        copyResource("acceptance/error/error_task.dig", root().resolve("error_task.dig"));
        CommandStatus status = main("run",
                "-o", root().toString(),
                "--project", root().toString(),
                "-p", "outdir=" + root(),
                "error_task.dig");
        assertThat(status.errUtf8(), status.code(), is(not(0)));
        assertThat(Files.exists(root().resolve("started.out")), is(true));
        assertThat(Files.exists(root().resolve("error1.out")), is(true));
        assertThat(Files.exists(root().resolve("error2.out")), is(true));
        assertThat(Files.exists(root().resolve("finished.out")), is(false));
    }

    @Test
    public void invalidErrorTask()
            throws Exception
    {
        copyResource("acceptance/error/invalid.dig", root().resolve("invalid.dig"));
        CommandStatus status = main("run",
                "-o", root().toString(),
                "--project", root().toString(),
                "-p", "outdir=" + root(),
                "invalid.dig");
        assertThat(status.errUtf8(), status.code(), is(not(0)));
        assertThat(Files.exists(root().resolve("started.out")), is(false));  // workflow should not start (validation should happen before starting the workflow)
        assertThat(status.errUtf8(), containsString("A task can't have more than one operator"));
    }
}
