package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.api.Id;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.Notification;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static utils.TestUtils.expectValue;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ShellNotificationIT
{
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JacksonTimeModule())
            .registerModule(new GuavaModule());

    private static final String PROJECT_NAME = "notification";
    private static final String WORKFLOW_NAME = "notification-test-wf";

    private final Path notificationFile;

    {
        try {
            notificationFile = Files.createTempDirectory("digdag-test").resolve("notification.json");
        }
        catch (IOException e) {
            throw ThrowablesUtil.propagate(e);
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "notification.type = shell",
                    "notification.shell.command = cat > " + notificationFile
            )
            .build();

    private Path config;
    private Path projectDir;

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
    }

    @Test
    public void testSessionFailureAlert()
            throws Exception
    {
        pushAndStart("acceptance/notification/fail.dig");

        Notification notification = expectValue(Duration.ofSeconds(30), () -> {
            if (!Files.exists(notificationFile)) {
                return null;
            }
            String notificationJson = new String(Files.readAllBytes(notificationFile), "UTF-8");
            return mapper.readValue(notificationJson, Notification.class);
        });

        assertThat(notification.getMessage(), is("Workflow session attempt failed"));
    }

    private Id pushAndStart(String workflow)
            throws IOException
    {
        TestUtils.copyResource(workflow, projectDir.resolve(WORKFLOW_NAME + ".dig"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                PROJECT_NAME,
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                PROJECT_NAME, WORKFLOW_NAME,
                "--session", "now");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

        return getAttemptId(startStatus);
    }
}
