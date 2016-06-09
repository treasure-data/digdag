package acceptance;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;

import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SlaIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private Path timeoutFile;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        timeoutFile = projectDir.resolve("timeout").toAbsolutePath().normalize();
    }

    @Test
    public void testTime()
            throws Exception
    {
        pushAndStart("time.dig", Duration.ofSeconds(5));
        expectTimeoutFile();
    }

    @Test
    public void testDuration()
            throws Exception
    {
        pushAndStart("duration.dig", Duration.ofSeconds(5));
        expectTimeoutFile();
    }

    private void pushAndStart(String workflow, TemporalAmount timeout)
            throws IOException
    {
        try (InputStream input = Resources.getResource("acceptance/sla/" + workflow).openStream()) {
            byte[] bytes = ByteStreams.toByteArray(input);
            String template = new String(bytes, "UTF-8");
            ZonedDateTime deadline = Instant.now().plus(timeout).atZone(ZoneOffset.UTC);
            String time = deadline.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String definition = template
                    .replace("${TIME}", time)
                    .replace("${TIMEOUT_FILE}", timeoutFile.toString());
            Files.write(projectDir.resolve("workflow.dig"), definition.getBytes("UTF-8"));
        }

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "sla",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "sla", "workflow",
                "--session", "now");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
    }

    private void expectTimeoutFile()
            throws InterruptedException
    {
        // Wait for the timeout file to come into existence
        for (int i = 0; i < 30; i++) {
            if (Files.exists(timeoutFile)) {
                break;
            }
            Thread.sleep(1000);
        }

        assertThat(Files.exists(timeoutFile), is(true));
    }
}
