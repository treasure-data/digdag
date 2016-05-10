package acceptance;

import com.google.common.io.ByteStreams;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.fakeHome;
import static acceptance.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class InitPushStartConfigIT
{
    private static final Pattern START_ATTEMPT_ID_PATTERN = Pattern.compile("\\s*id:\\s*(\\d+)\\s*");

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
        Path homedir = folder.newFolder("home").toPath();
        Path configDir = homedir.resolve(".digdag");
        Files.createDirectories(configDir);
        Path configFile = configDir.resolve("config");
        Path projectDir = tempdir.resolve("echo_params");
        Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/echo_params/digdag.yml", projectDir.resolve("digdag.yml"));
        copyResource("acceptance/echo_params/echo_params.yml", projectDir.resolve("echo_params.yml"));
        copyResource("acceptance/echo_params/scripts/__init__.py", scriptsDir.resolve("__init__.py"));
        copyResource("acceptance/echo_params/scripts/echo_params.py", scriptsDir.resolve("echo_params.py"));

        // Write a secret that we don't want the client to upload
        Files.write(configFile, "params.mysql.password=secret".getBytes(UTF_8));

        fakeHome(homedir.toString(), () -> {

            // Push the project
            CommandStatus pushStatus = main("push",
                    "echo_params",
                    "-c", config.toString(),
                    "-f", projectDir.resolve("digdag.yml").toString(),
                    "-e", server.endpoint(),
                    "-r", "4711");
            assertThat(pushStatus.code(), is(0));

            // Start the workflow
            long attemptId;
            {
                CommandStatus startStatus = main("start",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        "echo_params", "echo_params",
                        "--session", "now");
                assertThat(startStatus.code(), is(0));
                Matcher startAttemptIdMatcher = START_ATTEMPT_ID_PATTERN.matcher(startStatus.outUtf8());
                assertThat(startAttemptIdMatcher.find(), is(true));
                attemptId = Long.parseLong(startAttemptIdMatcher.group(1));
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

            String logs = getLogs(attemptId);
            assertThat(logs, containsString("digdag params"));
            assertThat(logs, not(containsString("secret")));
        });
    }

    private String getLogs(long attemptId)
            throws IOException
    {
        List<RestLogFileHandle> handles = client.getLogFileHandlesOfAttempt(attemptId);
        StringBuilder logs = new StringBuilder();
        for (RestLogFileHandle handle : handles) {
            try (InputStream s = new GZIPInputStream(client.getLogFile(attemptId, handle))) {
                logs.append(new String(ByteStreams.toByteArray(s), UTF_8));
            }
        }
        return logs.toString();
    }
}
