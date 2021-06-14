package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;

public class InitPushStartConfigIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void verifyDefaultConfigurationParamsAreNotUploadedIfConfigurationFileIsSpecified()
            throws Exception
    {
        Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        Path configDir = folder.getRoot().toPath().toAbsolutePath();
        Path configFile = configDir.resolve("config");
        Path projectDir = tempdir.resolve("echo_params");
        Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/echo_params/echo_params.dig", projectDir.resolve("echo_params.dig"));
        copyResource("acceptance/echo_params/scripts/__init__.py", scriptsDir.resolve("__init__.py"));
        copyResource("acceptance/echo_params/scripts/echo_params.py", scriptsDir.resolve("echo_params.py"));

        // Write a secret that we don't want the client to upload
        Files.write(configFile, "params.mysql.password=secret".getBytes(UTF_8));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "echo_params",
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
                    "echo_params", "echo_params",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        {
            RestSessionAttempt attempt = null;
            for (int i = 0; i < 30; i++) {
                attempt = client.getSessionAttempt(attemptId);
                if (attempt.getDone()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertThat(attempt.getSuccess(), is(true));
        }

        String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("digdag params"));
        assertThat(logs, not(containsString("secret")));
    }
}
