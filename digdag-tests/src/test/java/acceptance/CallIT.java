package acceptance;

import com.google.common.io.Resources;
import io.digdag.client.api.Id;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.expect;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;

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

    private void initCallProject()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.errUtf8(), initStatus.code(), is(0));

        String childWf = Resources.toString(Resources.getResource("acceptance/call/child.dig"), UTF_8)
                .replace("${outdir}", root().toString());

        copyResource("acceptance/call/parent.dig", projectDir.resolve("parent.dig"));
        Files.write(projectDir.resolve("child.dig"), childWf.getBytes(UTF_8));
        Files.createDirectories(projectDir.resolve("sub").resolve("subsub"));
        Files.write(projectDir.resolve("sub").resolve("child.dig"), childWf.getBytes(UTF_8));
        Files.write(projectDir.resolve("sub").resolve("subsub").resolve("child.dig"), childWf.getBytes(UTF_8));
    }

    @Test
    public void callSubdirLocal()
            throws Exception
    {
        initCallProject();

        // Run the workflow
        {
            CommandStatus startStatus = main("run",
                    "-c", config.toString(),
                    "--project", projectDir.toString(),
                    "parent.dig",
                    "-p", "outdir=" + root());
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        }

        // Verify that the file created by the child workflow is there
        assertThat(Files.exists(root().resolve("call_by_name.out")), is(true));
        assertThat(Files.exists(root().resolve("call_by_file.out")), is(true));
        assertThat(Files.exists(root().resolve("call_sub.out")), is(true));
        assertThat(Files.exists(root().resolve("call_subsub.out")), is(true));

        // Verify pwd of the scripts
        Path base = Paths.get(new String(Files.readAllBytes(root().resolve("call_by_name.out")), UTF_8).trim());
        assertThat(Paths.get(new String(Files.readAllBytes(root().resolve("call_sub.out")), UTF_8).trim()), is(base.resolve("sub")));
        assertThat(Paths.get(new String(Files.readAllBytes(root().resolve("call_subsub.out")), UTF_8).trim()), is(base.resolve("sub").resolve("subsub")));
    }

    @Test
    public void callSubdirServer()
            throws Exception
    {
        initCallProject();

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "call",
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        Id attemptId;
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
        assertThat(Files.exists(root().resolve("call_subsub.out")), is(true));

        // Verify pwd of the scripts
        Path subPath = Paths.get(new String(Files.readAllBytes(root().resolve("call_sub.out")), UTF_8).trim());
        Path subsubPath = Paths.get(new String(Files.readAllBytes(root().resolve("call_subsub.out")), UTF_8).trim());
        assertThat(subPath.getFileName(), is(Paths.get("sub")));
        assertThat(subsubPath.getFileName(), is(Paths.get("subsub")));
        assertThat(subsubPath.getParent().getFileName(), is(Paths.get("sub")));
    }
}
