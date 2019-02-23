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

public class LimitProjectArchiveFileSizeIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path projectDir;
    private Path config;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    @Test
    public void uploadProject()
            throws Exception
    {
        try (final TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .build()) {
            server.start();

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

            // Push the project
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.code(), is(0));
        }
    }

    @Test
    public void uploadProjectLargerThanLimit()
            throws Exception
    {
        try (final TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .configuration("api.max_archive_total_size_limit = 1")
                .build()) {
            server.start();

            // Create new project
            CommandStatus initStatus = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(initStatus.code(), is(0));

            copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.code(), is(1));
            assertThat(pushStatus.errUtf8(), containsString("Size of the uploaded archive file exceeds limit"));
        }
    }
}