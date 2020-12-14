package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.spi.Notification;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static utils.TestUtils.expect;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.startMockWebServer;

public class SlaIT
{
    protected static final String PROJECT_NAME = "sla";
    protected static final String WORKFLOW_NAME = "sla-test-wf";

    protected final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JacksonTimeModule())
            .registerModule(new GuavaModule());

    protected final MockWebServer mockWebServer = startMockWebServer();

    protected final String notificationUrl = "http://localhost:" + mockWebServer.getPort() + "/notification";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "notification.type = http",
                    "notification.http.url = " + notificationUrl
            )
            .build();

    protected Path config;
    protected Path projectDir;
    protected Path timeoutFile;
    protected DigdagClient client;

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

        timeoutFile = projectDir.resolve("timeout").toAbsolutePath().normalize();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (mockWebServer != null) {
            mockWebServer.shutdown();
        }
    }

    public static class TimeIT
            extends SlaIT
    {
        @Test
        public void testTimeCustomTask()
                throws Exception
        {
            pushAndStart("time_custom.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), () -> Files.exists(timeoutFile));
        }

        @Test
        public void testTimeFailDefault()
                throws Exception
        {
            Id attemptId = pushAndStart("time_fail_default.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
        }

        @Test
        public void testTimeFailEnabled()
                throws Exception
        {
            Id attemptId = pushAndStart("time_fail_enabled.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptFailure(server.endpoint(), attemptId));
        }

        @Test
        public void testTimeFailDisabled()
                throws Exception
        {
            Id attemptId = pushAndStart("time_fail_disabled.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
        }

        @Test
        public void testTimeAlertDefault()
                throws Exception
        {
            Id attemptId = pushAndStart("time_alert_default.dig", Duration.ofSeconds(5));
            expectNotification(attemptId, Duration.ofMinutes(5));
        }

        @Test
        public void testTimeAlertEnabled()
                throws Exception
        {
            Id attemptId = pushAndStart("time_alert_enabled.dig", Duration.ofSeconds(5));
            expectNotification(attemptId, Duration.ofMinutes(5));
        }

        @Test
        public void testTimeAlertDisabled()
                throws Exception
        {
            Id attemptId = pushAndStart("time_alert_disabled.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
            assertThat(mockWebServer.getRequestCount(), is(0));
        }
    }

    public static class DurationIT
            extends SlaIT
    {
        @Test
        public void testDurationCustomTask()
                throws Exception
        {
            pushAndStart("duration_custom.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), () -> Files.exists(timeoutFile));
        }

        @Test
        public void testDurationFailDefault()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_fail_default.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
        }

        @Test
        public void testDurationFailEnabled()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_fail_enabled.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptFailure(server.endpoint(), attemptId));
        }

        @Test
        public void testDurationFailDisabled()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_fail_disabled.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
        }

        @Test
        public void testDurationAlertDefault()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_alert_default.dig", Duration.ofSeconds(5));
            expectNotification(attemptId, Duration.ofMinutes(5));
        }

        @Test
        public void testDurationAlertEnabled()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_alert_enabled.dig", Duration.ofSeconds(5));
            expectNotification(attemptId, Duration.ofMinutes(5));
        }

        @Test
        public void testDurationAlertDisabled()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_alert_disabled.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
            assertThat(mockWebServer.getRequestCount(), is(0));
        }

        @Test
        public void slaShouldNotBeTriggeredEvenIfTotalDurationExceedsThreshold()
                throws Exception
        {
            Id attemptId = pushAndStart("duration_with_multiple_tasks.dig", Duration.ofSeconds(5));
            expect(Duration.ofMinutes(5), TestUtils.attemptSuccess(server.endpoint(), attemptId));
        }

        @Test
        public void verifyAlertIsRetried()
                throws Exception
        {
            mockWebServer.setDispatcher(new QueueDispatcher());
            mockWebServer.enqueue(new MockResponse().setResponseCode(500).setBody("FAIL"));
            mockWebServer.enqueue(new MockResponse().setResponseCode(200));
            Id attemptId = pushAndStart("duration_alert_enabled.dig", Duration.ofSeconds(5));
            RecordedRequest recordedRequest1 = mockWebServer.takeRequest(30, TimeUnit.SECONDS);
            RecordedRequest recordedRequest2 = mockWebServer.takeRequest(30, TimeUnit.SECONDS);
            verifyNotification(attemptId, recordedRequest1);
            verifyNotification(attemptId, recordedRequest2);
        }
    }

    protected void expectNotification(Id attemptId, Duration duration)
            throws InterruptedException, IOException
    {
        RecordedRequest recordedRequest = mockWebServer.takeRequest(duration.getSeconds(), TimeUnit.SECONDS);
        assertThat(recordedRequest, is(not(nullValue())));
        verifyNotification(attemptId, recordedRequest);
    }

    protected void verifyNotification(Id attemptId, RecordedRequest recordedRequest)
            throws IOException
    {
        String notificationJson = recordedRequest.getBody().readUtf8();
        Notification notification = mapper.readValue(notificationJson, Notification.class);
        assertThat(notification.getMessage(), is("SLA violation"));
        assertThat(Id.of(Long.toString(notification.getAttemptId().get())), is(attemptId));
        assertThat(notification.getWorkflowName().get(), is(WORKFLOW_NAME));
        assertThat(notification.getProjectName().get(), is(PROJECT_NAME));
    }

    protected Id pushAndStart(String workflow, TemporalAmount timeout)
            throws IOException
    {
        try (InputStream input = Resources.getResource("acceptance/sla/" + workflow).openStream()) {
            byte[] bytes = ByteStreams.toByteArray(input);
            String template = new String(bytes, "UTF-8");
            ZonedDateTime deadline = Instant.now().plus(timeout).atZone(ZoneOffset.UTC);
            String time = deadline.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String definition = template
                    .replace("${TIME}", time)
                    .replace("${TIMEOUT_FILE}", timeoutFile.toString());
            Files.write(projectDir.resolve(WORKFLOW_NAME + ".dig"), definition.getBytes("UTF-8"));
        }

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
