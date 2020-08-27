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

public class ParallelIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private String projectName;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        config = folder.newFile().toPath();

        projectName = "parallel";
        projectDir = folder.getRoot().toPath().resolve(projectName);
        Files.createDirectory(projectDir);

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void testParallelBasic()
            throws Exception
    {
        // Push the project
        copyResource("acceptance/parallel/parallel_basic.dig", projectDir.resolve("parallel_basic.dig"));
        CommandStatus pushStatus = pushProject();
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = startWorkflow("parallel_basic");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to success
        assertThat(joinAttempt(client, attemptId).getSuccess(), is(true));
        assertThat(getAttemptLogs(client, attemptId), containsString("task3"));
    }

    @Test
    public void testParallelLimit()
            throws Exception
    {
        // Push the project
        copyResource("acceptance/parallel/parallel_limit.dig", projectDir.resolve("parallel_limit.dig"));
        CommandStatus pushStatus = pushProject();
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = startWorkflow("parallel_limit");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to success
        assertThat(joinAttempt(client, attemptId).getSuccess(), is(true));
        assertThat(getAttemptLogs(client, attemptId), containsString("task3"));
    }

    @Test
    public void testParallelVariableGroup()
            throws Exception
    {
        // Push the project
        copyResource("acceptance/parallel/parallel_variable_group.dig", projectDir.resolve("parallel_variable_group.dig"));
        CommandStatus pushStatus = pushProject();
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(1));
    }

    @Test
    public void testParallelVariableLoop()
            throws Exception
    {
        // Push the project
        copyResource("acceptance/parallel/parallel_variable_loop.dig", projectDir.resolve("parallel_variable_loop.dig"));
        CommandStatus pushStatus = pushProject();
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = startWorkflow("parallel_variable_loop");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to success
        assertThat(joinAttempt(client, attemptId).getSuccess(), is(true));
        assertThat(getAttemptLogs(client, attemptId), containsString("loop1,task3"));
    }

    @Test
    public void testInvalidParallelVariableLoop()
            throws Exception
    {
        // Push the project
        copyResource("acceptance/parallel/invalid_parallel_variable_loop.dig", projectDir.resolve("invalid_parallel_variable_loop.dig"));
        CommandStatus pushStatus = pushProject();
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = startWorkflow("invalid_parallel_variable_loop");
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to fail
        assertThat(joinAttempt(client, attemptId).getSuccess(), is(false));
        assertThat(getAttemptLogs(client, attemptId), containsString("Expected 'true' or 'false' for key '_parallel' but"));
    }

    private CommandStatus pushProject()
    {
        return main("push",
                projectName,
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "-e", server.endpoint());
    }

    private CommandStatus startWorkflow(String workflowName)
    {
        return main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                projectName, workflowName,
                "--session", "now");
    }

    private RestSessionAttempt joinAttempt(DigdagClient client, Id attemptId)
            throws InterruptedException
    {
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 30; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        return attempt;
    }
}
