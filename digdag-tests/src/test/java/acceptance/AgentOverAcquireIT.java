package acceptance;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import utils.CommandStatus;

// Disabled to avoid too long execution time. We should remove this whole test in the future.
@Ignore
public class AgentOverAcquireIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path projectDir;
    private Path config;
    private Path outdir;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath();
        config = folder.newFile().toPath();

        outdir = projectDir.resolve("outdir");
        Files.createDirectories(outdir);
    }

    @Test
    public void testOverAcquire()
            throws Exception
    {
        copyResource("acceptance/over_acquire/over_acquire.dig", projectDir.resolve("over_acquire.dig"));

        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-X", "agent.heartbeat-interval=5",
                "-X", "agent.lock-retention-time=20",
                "-X", "agent.max-task-threads=5",
                "-p", "outdir=" + outdir,
                "over_acquire.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        for (int i = 0; i < 20; i++) {
            String one = new String(Files.readAllBytes(outdir.resolve(Integer.toString(i))), UTF_8).trim();
            assertThat(one, is("1"));
        }
    }
}
