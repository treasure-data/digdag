package acceptance;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.expect;
import static utils.TestUtils.main;
import static utils.TestUtils.attemptSuccess;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CallIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    @Test
    public void testCall()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.errUtf8(), initStatus.code(), is(0));

        copyResource("acceptance/call/parent.dig", projectDir.resolve("parent.dig"));
        copyResource("acceptance/call/child.dig", projectDir.resolve("child.dig"));
        Files.createDirectories(projectDir.resolve("sub"));
        copyResource("acceptance/call/child.dig", projectDir.resolve("sub").resolve("child.dig"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "call",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-p", "outdir=" + root());
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        long attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "call", "parent",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));

        // Verify that the file created by the child workflow is there
        assertThat(Files.exists(root().resolve("call_by_name.out")), is(true));
        assertThat(Files.exists(root().resolve("call_by_file.out")), is(true));
        assertThat(Files.exists(root().resolve("call_sub.out")), is(true));
    }
}
