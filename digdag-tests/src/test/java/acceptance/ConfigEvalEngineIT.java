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
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.*;

public class ConfigEvalEngineIT
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
        projectDir = folder.getRoot().toPath().resolve("config_eval_engine_test");
        Files.createDirectories(projectDir);
        config = folder.newFile().toPath();
    }


    @Test
    public void invalidConfigShouldNotInfiniteLoop()
            throws Exception
    {
        copyResource("acceptance/config_eval_engine/invalid1.dig", projectDir);
        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "config_eval_engine_test",
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        Id attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "config_eval_engine_test", "invalid1",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        expect(Duration.ofMinutes(5), attemptFailure(server.endpoint(), attemptId));

        // Verify that the file created by the child workflow is there
        //assertThat(Files.exists(root().resolve("call_by_name.out")), is(true));

    }
}
