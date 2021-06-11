package acceptance;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Path;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class LimitTaskCountIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp()
            throws Exception
    {
        System.setProperty("io.digdag.limits.maxWorkflowTasks", "10");
    }

    @After
    public void tearDown()
            throws Exception
    {
        System.getProperties().remove("io.digdag.limits.maxWorkflowTasks");
    }

    @Test
    public void verifyLoopBombsFail()
            throws Exception
    {
        // These fail due to limit checking by the loop operator
        verifyBombFails("single_small_loop_bomb.dig", "Too many loop subtasks.");
        verifyBombFails("single_large_loop_bomb.dig", "Too many loop subtasks.");
        verifyBombFails("parameterized_single_small_loop_bomb.dig", "Too many loop subtasks.");

        // These fail due to total workflow session task limiting by digdag core
        verifyBombFails("aggregate_loop_bomb.dig", "Too many tasks.");
        verifyBombFails("nested_loop_bomb.dig", "Too many tasks.");
        verifyBombFails("deep_nested_loop_bomb.dig", "Too many tasks.");
    }

    @Test
    public void verifyForEachBombsFail()
            throws Exception
    {
        // These fail due to limit checking by the for_each operator
        verifyBombFails("single_small_foreach_bomb.dig", "Too many for_each subtasks.");
        verifyBombFails("single_large_foreach_bomb.dig", "Too many for_each subtasks.");

        // These fail due to total workflow session task limiting by digdag core
        verifyBombFails("aggregate_foreach_bomb.dig", "Too many tasks.");
        verifyBombFails("nested_foreach_bomb.dig", "Too many tasks.");
        verifyBombFails("deep_nested_foreach_bomb.dig", "Too many tasks.");
    }

    private void verifyBombFails(String workflowFile, String needle)
            throws IOException
    {
        Path projectDir = newProjectDir();

        copyResource("acceptance/limit_task_count/" + workflowFile, projectDir.resolve("bomb.dig"));

        CommandStatus runStatus = main("run",
                "-c", "/dev/null",
                "-o", projectDir.toString(),
                "--project", projectDir.toString(),
                "bomb.dig");

        assertThat(runStatus.code(), is(not(0)));
        assertThat(runStatus.errUtf8(), runStatus.errUtf8(), containsString(needle));
    }

    private Path newProjectDir()
            throws IOException
    {
        return folder.newFolder().toPath().toAbsolutePath();
    }
}
