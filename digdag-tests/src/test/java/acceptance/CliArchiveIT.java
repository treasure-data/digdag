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

public class CliArchiveIT
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
    public void archiveProject()
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
            CommandStatus archiveStatus = main(
                    "archive",
                    "--project", projectDir.toString(),
                    "--output", "test_archive.tar.gz",
                    "-c", config.toString()
            );
            assertThat(archiveStatus.errUtf8(), archiveStatus.code(), is(0));
        }

        // 2. then upload it.
        {
            CommandStatus uploadStatus = main(
                    "upload",
                    "test_archive.tar.gz",
                    "test_archive_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(uploadStatus.errUtf8(), uploadStatus.code(), is(0));
        }
    }
}
