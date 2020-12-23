package acceptance;

import io.digdag.client.DigdagClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class TaskLimitIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server;

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    private void setUpServerWithSystemProps()
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .inProcess(false)  // setting system property doesn't work with in-process mode
                .systemProperty("io.digdag.limits.maxWorkflowTasks", "3")
                .build();
        server.start();
        setupClient();
    }

    private void setUpServerWithConfig()
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .inProcess(false)  // setting system property doesn't work with in-process mode
                .systemProperty("io.digdag.limits.maxWorkflowTasks", "100") //system property must be overridden by config
                .configuration("executor.task_max_run = 3")
                .build();
        server.start();
        setupClient();
    }

    private void setupClient()
    {
        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void attemptFailsWithTooManyTasks()
            throws Exception
    {
        setUpServerWithSystemProps();
        attemptFailsWithTooManyTasks0();
    }

    @Test
    public void attemptFailsWithTooManyTasksWithConfig()
            throws Exception
    {
        setUpServerWithConfig();
        attemptFailsWithTooManyTasks0();
    }

    private void attemptFailsWithTooManyTasks0()
            throws Exception
    {

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/task_limit/too_many_tasks.dig", projectDir.resolve("too_many_tasks.dig"));

        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "foobar", "too_many_tasks",
                "--session", "2016-01-01");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(1));
        assertThat(startStatus.errUtf8(), containsString("Too many tasks"));
    }
}
