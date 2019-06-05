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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.main;

public class DockerIT
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
    public void verifyDockerBuild()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("docker");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/docker/docker_build.dig", projectDir.resolve("docker_build.dig"));

        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "docker",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "docker", "docker_build",
                "--session", "now");
        assertThat(startStatus.code(), is(0));
        final Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to complete
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 30; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(attempt.getSuccess(), is(true));

        final String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("65536"));
    }

    @Test
    public void verifyDockerRunOptions()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("docker");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/docker/docker_run_options.dig", projectDir.resolve("docker_run_options.dig"));

        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "docker",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "docker", "docker_run_options",
                "--session", "now");
        assertThat(startStatus.code(), is(0));
        final Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to complete
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 30; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(attempt.getSuccess(), is(true));

        final String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("65536"));
    }

    @Test
    public void verifyPyOnDocker()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("py_docker");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/docker/docker_echo_params.dig", projectDir.resolve("docker_echo_params.dig"));
        copyResource("acceptance/echo_params/scripts/__init__.py", scriptsDir.resolve("__init__.py"));
        copyResource("acceptance/echo_params/scripts/echo_params.py", scriptsDir.resolve("echo_params.py"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "py_docker",
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
                    "py_docker", "docker_echo_params",
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
    }
}
