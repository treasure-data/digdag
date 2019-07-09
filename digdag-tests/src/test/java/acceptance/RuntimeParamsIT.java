package acceptance;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import io.digdag.client.api.Id;
import io.digdag.client.config.Config;

import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;

public class RuntimeParamsIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private static final ConfigFactory CF = TestUtils.configFactory();
    private static final YamlConfigLoader Y = new YamlConfigLoader();


    private Path config;
    private Path projectDir;
    private Path outFile;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        Files.createDirectories(projectDir);
        config = folder.newFile().toPath();

        outFile = projectDir.resolve("run.out").toAbsolutePath().normalize();

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.errUtf8(), initStatus.code(), is(0));
    }


    private void pushProject(String srcDigFileName)
            throws IOException
    {
        try (InputStream input = Resources.getResource("acceptance/runtime_params/" + srcDigFileName).openStream()) {
            String dig = new String(ByteStreams.toByteArray(input), "UTF-8")
                    .replace("__FILE__", outFile.toString());
            Files.write(projectDir.resolve(srcDigFileName), dig.getBytes("UTF-8"));
        }

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "runtime_params",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "3333"
        );
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
    }

    private Id startWorkflow(String workflowName, String retryName, String params)
    {
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "runtime_params", workflowName,
                "--session", "now",
                "--retry", retryName,
                "--param", params

        );
        assertThat(startStatus.code(), is(0));
        return getAttemptId(startStatus);
    }

    private void waitFinishAttempt(Id attemptId)
            throws InterruptedException
    {
        // Wait for the attempt to complete
        boolean success = false;
        for (int i = 0; i < 120; i++) {
            CommandStatus attemptsStatus = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            success = attemptsStatus.outUtf8().contains("status: success");
            if (success) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(success, is(true));
    }

    private Config loadYamlFile(Path name) throws IOException
    {
        return Y.loadFile(outFile.toFile()).toConfig(CF);
    }


    @Test
    public void testParamWithoutSchedule()
            throws Exception
    {
        String[] params = {
                "project_id=-1", "session_id=-2", "attempt_id=-3",
                "session_time=2010-02-02T11:22:33+00:00",
                "next_session_time=2010-03-03T11:22:33+00:00",
                "last_session_time=2010-01-01T11:22:33+00:00",
                "session_uuid=012345",
                "timezone=JST",
                "task_name=tasktasktask",
                "retry_attempt_name=retryretryretry"
        };

        pushProject("params1_no_sch.dig");
        Id attemptId = startWorkflow("params1_no_sch", "retry01", String.join(",", params));
        waitFinishAttempt(attemptId);

        // Verify that the file created by the child workflow is there
        assertThat(Files.exists(outFile), is(true));

        // load output of workflow as yaml.
        Config result = loadYamlFile(outFile);

        // These parameters must not be overwritten.
        assertNotEquals("-1", result.get("project_id", String.class));
        assertNotEquals("-2", result.get("session_id", String.class));
        assertNotEquals("-3", result.get("attempt_id", String.class));
        assertNotEquals("2010-02-02T11:22:33+00:00", result.get("session_time", String.class));
        assertNotEquals("012345", result.get("session_uuid", String.class));
        assertEquals("UTC", result.get("timezone", String.class));
        assertEquals("+params1_no_sch+t_task_name", result.get("task_name", String.class));
        assertEquals("retry01", result.get("retry_attempt_name", String.class));

        // These parameter is overwritten because no schedule.
        assertEquals("2010-03-03T11:22:33+00:00", result.get("next_session_time", String.class));
        assertEquals("2010-01-01T11:22:33+00:00", result.get("last_session_time", String.class));
    }

    @Test
    public void testParamWithSchedule()
            throws Exception
    {
        String[] params = {
                "project_id=-1", "session_id=-2", "attempt_id=-3",
                "session_time=2010-02-02T11:22:33+00:00",
                "next_session_time=2010-03-03T11:22:33+00:00",
                "last_session_time=2010-01-01T11:22:33+00:00",
                "session_uuid=012345",
                "timezone=JST",
                "task_name=tasktasktask",
                "retry_attempt_name=retryretryretry"
        };

        pushProject("params2_sch.dig");
        Id attemptId = startWorkflow("params2_sch", "retry01", String.join(",", params));
        waitFinishAttempt(attemptId);

        // Verify that the file created by the child workflow is there
        assertThat(Files.exists(outFile), is(true));

        // load output of workflow as yaml.
        Config result = loadYamlFile(outFile);

        // These parameters must not be overwritten.
        assertNotEquals("-1", result.get("project_id", String.class));
        assertNotEquals("-2", result.get("session_id", String.class));
        assertNotEquals("-3", result.get("attempt_id", String.class));
        assertNotEquals("2010-02-02T11:22:33+00:00", result.get("session_time", String.class));
        assertNotEquals("2010-03-03T11:22:33+00:00", result.get("next_session_time", String.class));
        assertNotEquals("2010-01-01T11:22:33+00:00", result.get("last_session_time", String.class));
        assertNotEquals("012345", result.get("session_uuid", String.class));
        assertEquals("UTC", result.get("timezone", String.class));
        assertEquals("+params2_sch+t_task_name", result.get("task_name", String.class));
        assertEquals("retry01", result.get("retry_attempt_name", String.class));
    }
}
