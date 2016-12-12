package io.digdag.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestDirectDownloadHandle;
import io.digdag.client.api.RestLogFileHandle;
import okhttp3.internal.tls.SslClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.InternalServerErrorException;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.List;

import static java.time.temporal.ChronoUnit.SECONDS;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
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
        mockWebServer.useHttps(SslClient.localhost().socketFactory, false);
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
        List<RestLogFileHandle> expectedLogFileHandles = ImmutableList.of(
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
        );

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(objectMapper.writeValueAsString(expectedLogFileHandles))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON));

        List<RestLogFileHandle> receivedLogFileHandles = client.getLogFileHandlesOfAttempt(Id.of("17")).getFiles();

        assertThat(receivedLogFileHandles, is(expectedLogFileHandles));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files"));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files"));
    }

    @Test
    public void getLogFileHandlesOfTask()
            throws Exception
    {
        List<RestLogFileHandle> expectedLogFileHandles = ImmutableList.of(
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
        );

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        mockWebServer.enqueue(new MockResponse()
                .setBody(objectMapper.writeValueAsString(expectedLogFileHandles))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON));

        List<RestLogFileHandle> receivedLogFileHandles = client.getLogFileHandlesOfTask(Id.of("17"), "test-task").getFiles();

        assertThat(receivedLogFileHandles, is(expectedLogFileHandles));

        assertThat(mockWebServer.getRequestCount(), is(2));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files?task=test-task"));
        assertThat(mockWebServer.takeRequest().getPath(), is("/api/logs/17/files?task=test-task"));
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

        String receivedLogFileContents = CharStreams.toString(new InputStreamReader(logFileStream));

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

        String receivedLogFileContents = CharStreams.toString(new InputStreamReader(logFileStream));

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
}