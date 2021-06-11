package acceptance;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import utils.CommandStatus;

public class BackgroundAfterIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private CommandStatus runResource(String name)
            throws IOException
    {
        copyResource("acceptance/background_after/" + name, root().resolve(name));

        return main("run",
                "-c", "/dev/null",
                "-o", root().toString(),
                "-p", "outdir=" + root(),
                "--project", root().toString(),
                name);
    }

    @Test
    public void backgroundTask()
            throws IOException
    {
        CommandStatus runStatus = runResource("background_task.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(runStatus.errUtf8(), not(containsString("is not used")));
        assertThat(runStatus.outUtf8(), not(containsString("is not used")));

        assertOutputContents("background_task.txt", "ok");
    }

    @Test
    public void backgroundGroup()
            throws IOException
    {
        CommandStatus runStatus = runResource("background_group.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(runStatus.errUtf8(), not(containsString("is not used")));
        assertThat(runStatus.outUtf8(), not(containsString("is not used")));

        assertOutputContents("background_group.txt", "ok");
    }

    @Test
    public void rejectBackgroundWithParallel()
            throws IOException
    {
        CommandStatus runStatus = runResource("background_with_parallel.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), not(is(0)));
        assertThat(runStatus.errUtf8(), containsString("error: Setting \"_background: true\" option is invalid (unnecessary) if its parent task has \"_parallel: true\" option"));
    }

    @Test
    public void afterTask()
            throws IOException
    {
        CommandStatus runStatus = runResource("after_task.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(runStatus.errUtf8(), not(containsString("is not used")));
        assertThat(runStatus.outUtf8(), not(containsString("is not used")));

        assertOutputContents("after_task.txt", "ok");
    }

    @Test
    public void afterGroup()
            throws IOException
    {
        CommandStatus runStatus = runResource("after_group.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));
        assertThat(runStatus.errUtf8(), not(containsString("is not used")));
        assertThat(runStatus.outUtf8(), not(containsString("is not used")));

        assertOutputContents("after_group.txt", "ok");
    }

    @Test
    public void rejectAfterWithoutParallel()
            throws IOException
    {
        CommandStatus runStatus = runResource("after_without_parallel.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), not(is(0)));
        assertThat(runStatus.errUtf8(), containsString("error: Setting \"_after\" option is invalid if its parent task doesn't have \"_parallel: true\" option"));
    }

    private void assertOutputContents(String name, String contents)
            throws IOException
    {
        assertThat(
                new String(Files.readAllBytes(root().resolve(name)), UTF_8).trim(),
                is(contents));
    }
}
