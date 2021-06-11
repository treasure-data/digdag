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
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.main;

public class RbIT
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
    public void verifyConfigurationPararms()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("rb");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/echo_params/echo_rb_params.dig", projectDir.resolve("echo_rb_params.dig"));
        copyResource("acceptance/echo_params/scripts/echo_params.rb", scriptsDir.resolve("echo_params.rb"));

        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "rb",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "rb", "echo_rb_params",
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
        assertThat(logs, containsString("digdag params"));
    }

    @Test
    public void verifyConfigurationRubyOption()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("rb");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/rb/config_ruby.dig", projectDir.resolve("config_ruby.dig"));
        copyResource("acceptance/echo_params/scripts/echo_params.rb", scriptsDir.resolve("echo_params.rb"));

        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "rb",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "rb", "config_ruby",
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
        assertThat(logs, containsString("ruby ruby"));
        assertThat(logs, containsString("ruby [\"ruby\", \"-w\"]"));
    }

    @Test
    public void testRubyErrorMessageAndStacktrace()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("rb");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/rb/stacktrace_ruby.dig", projectDir.resolve("stacktrace_ruby.dig"));
        copyResource("acceptance/rb/scripts/stacktrace_ruby.rb", scriptsDir.resolve("stacktrace_ruby.rb"));

        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "rb",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "rb", "stacktrace_ruby",
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

        final String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("Task failed with unexpected error: Ruby command failed with code 1: my error message (StacktraceRuby::MyErrorClass)"));
        assertThat(logs, containsString(":in `private_run'"));
        assertThat(logs, containsString(":in `run'"));
        assertThat(logs, containsString(":in `<main>'"));
        assertThat(logs, containsString("ERROR_MESSAGE_BEGIN Ruby command failed with code 1: my error message (StacktraceRuby::MyErrorClass)"));
    }
}
