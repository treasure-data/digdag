package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.spi.Notification;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushProject;
import static utils.TestUtils.startMockWebServer;
import static utils.TestUtils.startWorkflow;

public class ExecutionTimeoutIT
{
    private static final String WORKFLOW_NAME = "timeout_test_wf";
    private static final String PROJECT_NAME = "timeout_test_proj";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JacksonTimeModule())
            .registerModule(new GuavaModule());

    protected TemporaryDigdagServer server;

    protected Path projectDir;
    protected DigdagClient client;

    private MockWebServer notificationServer;

    private String notificationUrl;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.newFolder().toPath();
        notificationServer = startMockWebServer();
        notificationUrl = "http://localhost:" + notificationServer.getPort() + "/notification";
    }

    protected void setup(String... configuration)
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .configuration(
                        "notification.type = http",
                        "notification.http.url = " + notificationUrl
                )
                .configuration(configuration)
                .inProcess(false)
                .build();

        server.start();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @After
    public void tearDownServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @After
    public void tearDownWebServer()
            throws Exception
    {
        if (notificationServer != null) {
            notificationServer.shutdown();
            notificationServer = null;
        }
    }

    public static class AttemptTimeoutIT
            extends ExecutionTimeoutIT
    {
        @Test
        public void testAttemptTimeout()
                throws Exception
        {
            setup("executor.attempt_ttl = 10s",
                    "executor.task_ttl = 1d",
                    "executor.ttl_reaping_interval = 1s");

            addWorkflow(projectDir, "acceptance/attempt_timeout/attempt_timeout.dig", WORKFLOW_NAME + ".dig");
            pushProject(server.endpoint(), projectDir, PROJECT_NAME);
            Id attemptId = startWorkflow(server.endpoint(), PROJECT_NAME, WORKFLOW_NAME);

            // Expect the attempt to get canceled
            expect(Duration.ofMinutes(4), () -> client.getSessionAttempt(attemptId).getCancelRequested(), Duration.ofSeconds(10));

            // And then the attempt should be done pretty soon
            expect(Duration.ofMinutes(4), () -> client.getSessionAttempt(attemptId).getDone(), Duration.ofSeconds(10));

            // Expect a notification to be sent
            expectNotification(attemptId, Duration.ofMinutes(2), "Workflow execution timeout"::equals);

            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            assertThat(attempt.getDone(), is(true));
            assertThat(attempt.getCancelRequested(), is(true));
            assertThat(attempt.getSuccess(), is(false));
        }
    }

    public static class TaskTimeoutIT
            extends ExecutionTimeoutIT
    {
        @Test
        public void testTaskTimeout()
                throws Exception
        {
            setup("executor.attempt_ttl = 1d",
                    "executor.task_ttl = 10s",
                    "executor.ttl_reaping_interval = 1s");

            addWorkflow(projectDir, "acceptance/attempt_timeout/task_timeout.dig", WORKFLOW_NAME + ".dig");
            pushProject(server.endpoint(), projectDir, PROJECT_NAME);
            Id attemptId = startWorkflow(server.endpoint(), PROJECT_NAME, WORKFLOW_NAME);

            // Expect the attempt to get canceled when the task times out
            expect(Duration.ofMinutes(2), () -> client.getSessionAttempt(attemptId).getCancelRequested());

            // Expect a notification to be sent
            expectNotification(attemptId, Duration.ofMinutes(2), message -> Pattern.matches("Task execution timeout: \\d+", message));

            // TODO: implement termination of blocking tasks
            // TODO: verify that blocking tasks are terminated when the attempt is canceled

            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            assertThat(attempt.getCancelRequested(), is(true));
        }
    }

    public static class TaskNotTimeoutIT
            extends ExecutionTimeoutIT
    {
        @Test
        public void testTaskNotTimeout()
                throws Exception
        {
            setup("executor.attempt_ttl = 25s",
                    "executor.task_ttl = 20s",
                    "executor.ttl_reaping_interval = 1s");

            addWorkflow(projectDir, "acceptance/attempt_timeout/task_not_timeout.dig", WORKFLOW_NAME + ".dig");
            pushProject(server.endpoint(), projectDir, PROJECT_NAME);
            Id attemptId = startWorkflow(server.endpoint(), PROJECT_NAME, WORKFLOW_NAME);

            // Expect the attempt to get canceled
            expect(Duration.ofMinutes(2), () -> client.getSessionAttempt(attemptId).getCancelRequested());

            // Expect a notification to be sent with attempt's timeout message
            expectNotification(attemptId, Duration.ofMinutes(2), "Workflow execution timeout"::equals);

            RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
            assertThat(attempt.getCancelRequested(), is(true));
        }
    }

    protected void expectNotification(Id attemptId, Duration duration, Predicate<String> messageMatcher)
            throws InterruptedException, IOException
    {
        RecordedRequest recordedRequest = notificationServer.takeRequest(duration.getSeconds(), TimeUnit.SECONDS);
        assertThat(recordedRequest, is(not(nullValue())));
        verifyNotification(attemptId, recordedRequest, messageMatcher);
    }

    protected void verifyNotification(Id attemptId, RecordedRequest recordedRequest, Predicate<String> messageMatcher)
            throws IOException
    {
        String notificationJson = recordedRequest.getBody().readUtf8();
        Notification notification = mapper.readValue(notificationJson, Notification.class);
        assertThat(notification.getMessage(), messageMatcher.test(notification.getMessage()), is(true));
        assertThat(Id.of(Long.toString(notification.getAttemptId().get())), is(attemptId));
        assertThat(notification.getWorkflowName().get(), is(WORKFLOW_NAME));
        assertThat(notification.getProjectName().get(), is(PROJECT_NAME));
    }
}
