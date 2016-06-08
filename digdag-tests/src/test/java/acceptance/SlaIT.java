package acceptance;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static acceptance.TestUtils.getAttemptId;
import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
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

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    @Test
    public void testTime()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        try (InputStream input = Resources.getResource("acceptance/sla/time.dig").openStream()) {
            byte[] bytes = ByteStreams.toByteArray(input);
            String definition = new String(bytes, "UTF-8");
            ZonedDateTime deadline = Instant.now().plusSeconds(10).atZone(ZoneOffset.UTC);
            String time = deadline.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String definitionWithDeadline = definition.replace("$TIME", time);
            Files.write(projectDir.resolve("time.dig"), definitionWithDeadline.getBytes("UTF-8"));
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
                "sla", "time",
                "--session", "now");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        long attemptId = getAttemptId(startStatus);

        // Wait for the attempt to complete
        CommandStatus attemptsStatus = null;
        for (int i = 0; i < 30; i++) {
            attemptsStatus = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.errUtf8(), attemptsStatus.code(), is(0));
            if (!attemptsStatus.outUtf8().contains("status: running")) {
                break;
            }
            Thread.sleep(1000);
        }

        assertThat(attemptsStatus.outUtf8(), containsString("status: error"));
    }
}
