package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.isNotNull;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.main;

public class PyIT
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
        Path projectDir = tempdir.resolve("py");
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

        RestSessionAttempt attempt = pushAndStart(projectDir, "echo_params", "4709");
        assertThat(attempt, notNullValue());
        assertThat(attempt.getSuccess(), is(true));

        String logs = getAttemptLogs(client, attempt.getId());
        assertThat(logs, containsString("digdag params"));
    }

    @Test
    public void verifyConfigurationPythonOption()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("py");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/py/config_python.dig", projectDir.resolve("config_python.dig"));
        copyResource("acceptance/echo_params/scripts/__init__.py", scriptsDir.resolve("__init__.py"));
        copyResource("acceptance/echo_params/scripts/echo_params.py", scriptsDir.resolve("echo_params.py"));

        RestSessionAttempt attempt = pushAndStart(projectDir, "config_python", "4710");
        assertThat(attempt, notNullValue());
        assertThat(attempt.getSuccess(), is(true));

        final String logs = getAttemptLogs(client, attempt.getId());
        assertThat(logs, containsString("python python"));
        assertThat(logs, containsString("python [u'python', u'-v']"));
    }

    @Test
    public void testPythonErrorMessageAndStacktrace()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("py");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/py/stacktrace_python.dig", projectDir.resolve("stacktrace_python.dig"));
        copyResource("acceptance/py/scripts/stacktrace.py", scriptsDir.resolve("stacktrace.py"));
        copyResource("acceptance/py/scripts/__init__.py", scriptsDir.resolve("__init__.py"));

        RestSessionAttempt attempt = pushAndStart(projectDir, "stacktrace_python", "4711");
        assertThat(attempt, notNullValue());
        assertThat(attempt.getSuccess(), is(false));

        final String logs = getAttemptLogs(client, attempt.getId());
        assertThat(logs, containsString("Task failed with unexpected error: Python command failed with code 1"));
        assertThat(logs, containsString(", in run"));
        assertThat(logs, containsString("ERROR_MESSAGE_BEGIN Python command failed with code 1"));
    }

    @Test
    public void verifyLogWithSyntaxError()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("py");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/py/syntax_error_python.dig", projectDir.resolve("syntax_error_python.dig"));
        copyResource("acceptance/py/scripts/syntax_error.py", scriptsDir.resolve("syntax_error.py"));
        copyResource("acceptance/py/scripts/__init__.py", scriptsDir.resolve("__init__.py"));

        RestSessionAttempt attempt = pushAndStart(projectDir, "syntax_error_python", "4712");
        assertThat(attempt, notNullValue());
        assertThat(attempt.getSuccess(), is(false));

        final String logs = getAttemptLogs(client, attempt.getId());
        assertTrue(logs != null);
        assertThat(attempt.getSuccess(), is(false));
        final String regex = "\\[ERROR\\] [^\\n]*Task failed with unexpected error: Python command failed with code 1.*Error messages from python:[^\\n]*\\(SyntaxError\\)";
        assertTrue(Pattern.compile(regex, Pattern.DOTALL).matcher(logs).find());
    }

    @Test
    public void verifyLogWithDuplicateModuleError()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("py");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/py/dup_module.dig", projectDir.resolve("dup_module.dig"));
        copyResource("acceptance/py/scripts/tokenize.py", projectDir.resolve("tokenize.py"));

        RestSessionAttempt attempt = pushAndStart(projectDir, "dup_module", "4713");
        assertThat(attempt, notNullValue());
        final String logs = getAttemptLogs(client, attempt.getId());
        assertTrue(logs != null);
        assertThat(attempt.getSuccess(), is(false));
        final String regex = "\\[ERROR\\] [^\\n]*Task failed with unexpected error: Python command failed with code 1.*Error messages from python:[^\\n]*duplicae module name with standard library";
        assertTrue(Pattern.compile(regex, Pattern.DOTALL).matcher(logs).find());
    }

    private RestSessionAttempt pushAndStart(Path projectDir, String workflowName, String revision)
            throws Exception
    {
        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "py",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", revision);
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "py", workflowName,
                "--session", "now");
        assertThat(startStatus.code(), is(0));
        final Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to complete
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 60; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        return attempt;
    }
}
