package acceptance;

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
    public void invalidErrorTask()
            throws Exception
    {
        copyResource("acceptance/error/invalid.dig", root().resolve("invalid.dig"));
        CommandStatus status = main("run", "-o", root().toString(), "--project", root().toString(), "invalid.dig");
        assertThat(status.errUtf8(), status.code(), is(not(0)));
        assertThat(status.outUtf8(), not(containsString("Workflow started")));  // workflow should not start (validation should happen before starting the workflow)
        assertThat(status.errUtf8(), containsString("A task can't have more than one operator"));
    }
}
