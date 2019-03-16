package acceptance;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

//
// This file doesn't contain normal case.
// It defined in another test.
//
public class ValidateProjectIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .build();

    private Path config;
    private Path projectDir;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    @Test
    public void uploadInvalidTaskProject()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/error_task/invalid_at_group.dig", projectDir.resolve("invalid_at_group.dig"));

        // Push the project
        CommandStatus pushStatus = main(
                "push",
                "--project", projectDir.toString(),
                "foobar",
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushStatus.code(), is(1));
        assertThat(pushStatus.errUtf8(), containsString("A task can't have more than one operator"));
    }

    @Test
    public void uploadInvalidScheduleProject()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/schedule/invalid_schedule.dig", projectDir.resolve("invalid_schedule.dig"));

        // Push the project
        CommandStatus pushStatus = main(
                "push",
                "--project", projectDir.toString(),
                "foobar",
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushStatus.code(), is(1));
        assertThat(pushStatus.errUtf8(), containsString("scheduler requires mm:ss format"));
    }
}
