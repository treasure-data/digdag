package io.digdag.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestDirectDownloadHandle;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestLogFileHandleCollection;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.tls.HandshakeCertificates;
import org.bouncycastle.util.io.Streams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.WebApplicationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;

import static io.digdag.client.DigdagVersion.buildVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.SECONDS;
import static javax.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.HttpHeaders.USER_AGENT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static okhttp3.tls.internal.TlsUtil.localhost;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DigdagClientTest
{
    private MockWebServer mockWebServer;
    private DigdagClient client;
    private ObjectMapper objectMapper;

    @Before
    public void setUp()
            throws Exception
    {
        mockWebServer = new MockWebServer();
        HandshakeCertificates handshakeCertificates = localhost();
        SSLSocketFactory socketFactory = handshakeCertificates.sslSocketFactory();
        mockWebServer.useHttps(socketFactory, false);
        mockWebServer.start();

        client = DigdagClient.builder()
                .disableCertValidation(true)
                .ssl(true)
                .host(mockWebServer.getHostName())
                .port(mockWebServer.getPort())
                .build();

        objectMapper = DigdagClient.objectMapper();
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (mockWebServer != null) {
            mockWebServer.shutdown();
        }
    }

    @Test
    public void getLogFileHandlesOfAttempt()
            throws Exception
    {
        RestLogFileHandleCollection expectedLogFileHandles = RestLogFileHandleCollection.builder()
                .addFiles(
                        RestLogFileHandle.builder()
                                .agentId("test-agent")
                                .fileName("test-task-1.log")
                                .fileSize(4711)
                                .fileTime(Instant.now().truncatedTo(SECONDS))
                                .taskName("test-task-1")
                                .build(),
                        RestLogFileHandle.builder()
                                .agentId("test-agent")
                                .fileName("test-task-2.log")
                                .fileSize(4712)
                                .fileTime(Instant.now().truncatedTo(SECONDS))
                                .taskName("test-task-2")
                                .build()
                ).build();

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(objectMapper.writeValueAsString(expectedLogFileHandles))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON));

        RestLogFileHandleCollection receivedLogFileHandles = client.getLogFileHandlesOfAttempt(Id.of("17"));

        assertThat(receivedLogFileHandles, is(expectedLogFileHandles));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files"));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files"));
    }

    @Test
    public void getLogFileHandlesOfTask()
            throws Exception
    {
        RestLogFileHandleCollection expectedLogFileHandles = RestLogFileHandleCollection.builder()
                .addFiles(
                        RestLogFileHandle.builder()
                                .agentId("test-agent")
                                .fileName("test-task-1.log")
                                .fileSize(4711)
                                .fileTime(Instant.now().truncatedTo(SECONDS))
                                .taskName("test-task-1")
                                .build(),
                        RestLogFileHandle.builder()
                                .agentId("test-agent")
                                .fileName("test-task-2.log")
                                .fileSize(4712)
                                .fileTime(Instant.now().truncatedTo(SECONDS))
                                .taskName("test-task-2")
                                .build()
                ).build();

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(objectMapper.writeValueAsString(expectedLogFileHandles))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON));

        RestLogFileHandleCollection receivedLogFileHandles = client.getLogFileHandlesOfTask(Id.of("17"), "test-task");

        assertThat(receivedLogFileHandles, is(expectedLogFileHandles));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files?task=test-task"));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files?task=test-task"));
    }

    @Test
    public void getLogFileHandlesOfTaskURLEncodedTaskName()
            throws Exception
    {
        RestLogFileHandleCollection expectedLogFileHandles = RestLogFileHandleCollection.builder()
                .addFiles(
                        RestLogFileHandle.builder()
                                .agentId("test-agent")
                                .fileName("test-task-%{}-1.log")
                                .fileSize(4711)
                                .fileTime(Instant.now().truncatedTo(SECONDS))
                                .taskName("test-task-%{}-1")
                                .build(),
                        RestLogFileHandle.builder()
                                .agentId("test-agent")
                                .fileName("test-task-%{}-2.log")
                                .fileSize(4712)
                                .fileTime(Instant.now().truncatedTo(SECONDS))
                                .taskName("test-task-%{}-2")
                                .build()
                ).build();

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(objectMapper.writeValueAsString(expectedLogFileHandles))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON));

        RestLogFileHandleCollection receivedLogFileHandles = client.getLogFileHandlesOfTask(Id.of("17"), "test-task-%{}");

        assertThat(receivedLogFileHandles, is(expectedLogFileHandles));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files?task=test-task-%25%7B%7D"));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files?task=test-task-%25%7B%7D"));
    }

    @Test
    public void getLogFileDirect()
            throws Exception
    {
        Id attemptId = Id.of("17");
        String logFileContents = "foo\nbar";
        String logFilePath = "/logs/test-task-1.log";
        String logFileUrl = "https://" + mockWebServer.getHostName() + ":" + mockWebServer.getPort() + logFilePath;

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(logFileContents)
                .setHeader(CONTENT_TYPE, TEXT_PLAIN));

        InputStream logFileStream = client.getLogFile(attemptId, RestLogFileHandle.builder()
                .direct(RestDirectDownloadHandle.of(logFileUrl))
                .agentId("test-agent")
                .fileName("test-task-1.log")
                .fileSize(4711)
                .fileTime(Instant.now().truncatedTo(SECONDS))
                .taskName("test-task-1")
                .build());

        String receivedLogFileContents = CharStreams.toString(new InputStreamReader(logFileStream, UTF_8));

        assertThat(receivedLogFileContents, is(logFileContents));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is(logFilePath));
        assertThat(mockWebServer.takeRequest().getPath(), is(logFilePath));
    }

    @Test
    public void getLogFile()
            throws Exception
    {
        int attemptId = 17;
        String logFileContents = "foo\nbar";
        String logFileName = "test-task-1.log";
        String logFilePath = "/api/logs/" + attemptId + "/files/" + logFileName;

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(logFileContents)
                .setHeader(CONTENT_TYPE, TEXT_PLAIN));

        InputStream logFileStream = client.getLogFile(Id.of("17"), RestLogFileHandle.builder()
                .agentId("test-agent")
                .fileName(logFileName)
                .fileSize(4711)
                .fileTime(Instant.now().truncatedTo(SECONDS))
                .taskName("test-task-1")
                .build());

        String receivedLogFileContents = CharStreams.toString(new InputStreamReader(logFileStream, UTF_8));

        assertThat(receivedLogFileContents, is(logFileContents));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is(logFilePath));
        assertThat(mockWebServer.takeRequest().getPath(), is(logFilePath));
    }

    @Test
    public void getLogFileFailsAfter10Attempts()
            throws Exception
    {
        QueueDispatcher dispatcher = new QueueDispatcher();
        dispatcher.setFailFast(new MockResponse().setResponseCode(500));
        mockWebServer.setDispatcher(dispatcher);

        try {
            client.getLogFile(Id.of("17"), RestLogFileHandle.builder()
                    .agentId("test-agent")
                    .fileName("test-task-1.log")
                    .fileSize(4711)
                    .fileTime(Instant.now().truncatedTo(SECONDS))
                    .taskName("test-task-1")
                    .build());
            fail();
        }
        catch (InternalServerErrorException ignore) {
        }

        assertThat(mockWebServer.getRequestCount(), is(10));
    }

    @Test
    public void testUserAgent()
            throws Exception
    {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200)
                .setHeader(CONTENT_TYPE, "application/json")
                .setBody("{\"version\":\"1.2.3\"}"));

        client.getVersion();

        assertThat(mockWebServer.getRequestCount(), is(1));
        RecordedRequest request = mockWebServer.takeRequest();

        assertThat(request.getHeader(USER_AGENT), is("DigdagClient/" + buildVersion()));
    }

    @Test
    public void testAcceptEncoding()
            throws InterruptedException
    {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200)
                .setHeader(CONTENT_TYPE, "application/json")
                .setBody("{\"version\":\"1.2.3\"}"));

        client.getVersion();

        RecordedRequest request = mockWebServer.takeRequest();

        assertThat(request.getHeader(ACCEPT_ENCODING), is("gzip, deflate"));
    }

    @Test
    public void getProjectArchiveWithRedirect()
            throws IOException, InterruptedException
    {
        String redirectUrl = String.format("https://%s:%d/redirect/pathname", mockWebServer.getHostName(), mockWebServer.getPort());
        mockWebServer.enqueue(new MockResponse().setResponseCode(303).setHeader("Location", redirectUrl));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("foobar"));
        InputStream archive = client.getProjectArchive(Id.of("42"), "12345678-abcdef");
        assertArrayEquals("foobar".getBytes(UTF_8), Streams.readAll(archive));
        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/projects/42/archive?revision=12345678-abcdef"));
        assertThat(mockWebServer.takeRequest().getPath(), is("/redirect/pathname"));
    }

    @Test
    public void getProjectArchiveWithRedirect10Times()
            throws IOException, InterruptedException
    {
        String redirectUrl = String.format("https://%s:%d/redirect/pathname", mockWebServer.getHostName(), mockWebServer.getPort());
        for (int i = 0; i < 10; i++) {
            mockWebServer.enqueue(new MockResponse().setResponseCode(303).setHeader("Location", redirectUrl));
        }
        try {
            client.getProjectArchive(Id.of("42"), "12345678-abcdef");
            assertTrue(false);
        }
        catch (WebApplicationException e) {
            assertThat(mockWebServer.getRequestCount(), is(10));
            assertThat(mockWebServer.takeRequest().getPath(), is("/api/projects/42/archive?revision=12345678-abcdef"));
            for (int i = 0; i < 10 - 1; i++) {
                assertThat(mockWebServer.takeRequest().getPath(), is("/redirect/pathname"));
            }
        }
    }
}
