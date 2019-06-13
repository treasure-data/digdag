package acceptance;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class CliStartIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

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
    public void startProject()
            throws Exception
    {

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        // 1. archive project
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "test_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint()
            );
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // 2. then start it.
        {
            CommandStatus uploadStatus = main(
                    "start",
                    "test_proj",
                    "--session", "now",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(uploadStatus.errUtf8(), uploadStatus.code(), is(0));
        }
    }
}
