package acceptance;

import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import utils.CommandStatus;

import static java.nio.charset.StandardCharsets.UTF_8;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RunParamsVisibilityIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void runAll()
            throws IOException
    {
        copyResource("acceptance/params_visibility/helper.py", root().resolve("helper.py"));
    }

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private void assertOutputContents(String name, String contents)
            throws IOException
    {
        assertThat(
                new String(Files.readAllBytes(root().resolve(name + ".out")), UTF_8).trim(),
                is(contents));
    }

    @Test
    public void testParams()
            throws Exception
    {
        copyResource("acceptance/params_visibility/params_visibility.dig", root().resolve("params_visibility.dig"));
        CommandStatus runStatus = main("run", "-o", root().toString(), "--project", root().toString(), "params_visibility.dig", "--session", "2016-01-01 00:00:00");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        assertOutputContents("simple_export", "simple_export");
        assertOutputContents("export_overwrite", "export_overwrite");
        assertOutputContents("simple_store", "simple_store");
        assertOutputContents("store_overwrite", "store_overwrite");
        assertOutputContents("dag_store_join", "c");
        assertOutputContents("parallel_store_fork_a", "a");
        assertOutputContents("parallel_store_fork_b", "b");
        assertOutputContents("parallel_store_fork_c", "prepare");
        assertOutputContents("parallel_store_fork_join", "b");
    }

    @Test
    public void testDynamicTasksParams()
            throws Exception
    {
        copyResource("acceptance/params_visibility/dynamic_tasks_visibility.dig", root().resolve("dynamic_tasks_visibility.dig"));
        CommandStatus runStatus = main("run", "-o", root().toString(), "--project", root().toString(), "dynamic_tasks_visibility.dig", "--session", "2016-01-01 00:00:00");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        for (int outer = 1; outer <= 5; outer++) {
            for (int inner = 1; inner <= 5; inner++) {
                assertOutputContents("parallel_get_first_" + outer + "_" + inner, "initial");
                assertOutputContents("parallel_override_" + outer + "_" + inner, "override_" + outer + "_" + inner);
            }
        }
    }
}
