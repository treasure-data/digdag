package acceptance;

import io.digdag.client.api.Id;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class RequireIT
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
        Files.createDirectories(projectDir);
        config = folder.newFile().toPath();
    }

    @Test
    public void testRequire()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.errUtf8(), initStatus.code(), is(0));

        Path childOutFile = projectDir.resolve("child.out").toAbsolutePath().normalize();

        try (InputStream input = Resources.getResource("acceptance/require/child.dig").openStream()) {
            String child = new String(ByteStreams.toByteArray(input), "UTF-8")
                    .replace("__FILE__", childOutFile.toString());
            Files.write(projectDir.resolve("child.dig"), child.getBytes("UTF-8"));
        }
        copyResource("acceptance/require/parent.dig", projectDir.resolve("parent.dig"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "require",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        Id attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "require", "parent",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        boolean success = false;
        for (int i = 0; i < 30; i++) {
            CommandStatus attemptsStatus = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            success = attemptsStatus.outUtf8().contains("status: success");
            if (success) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(success, is(true));

        // Verify that the file created by the child workflow is there
        assertThat(Files.exists(childOutFile), is(true));
    }

    @Test
    public void testRequireFailsWhenDependentFails()
            throws Exception
    {
        copyResource("acceptance/require/parent.dig", projectDir.resolve("parent.dig"));
        copyResource("acceptance/require/fail.dig", projectDir.resolve("child.dig"));

        CommandStatus status = main("run",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "parent.dig");
        assertThat(status.errUtf8(), status.code(), is(not(0)));

        assertThat(status.errUtf8(), containsString("Dependent workflow failed."));
    }

    @Test
    public void testRequireSucceedsWhenDependentFailsButIgnoreFailureIsSet()
            throws Exception
    {
        copyResource("acceptance/require/parent_ignore_failure.dig", projectDir.resolve("parent.dig"));
        copyResource("acceptance/require/fail.dig", projectDir.resolve("child.dig"));

        CommandStatus status = main("run",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "parent.dig");
        assertThat(status.errUtf8(), status.code(), is(0));
    }
}
