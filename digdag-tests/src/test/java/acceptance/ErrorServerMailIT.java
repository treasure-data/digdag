package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.startMailServer;

public class ErrorServerMailIT
{
    private static final String SENDER = "alert@digdag.io";
    private static final String RECEIVER = "test@digdag.io";
    private static final String LOCAL_SESSION_TIME = "2016-01-02 03:04:05";
    private static final String SESSION_TIME_ISO = "2016-01-02T03:04:05+00:00";
    private static final String HOSTNAME = "127.0.0.1";
    private static final String SMTP_USER = "mail-user";
    private static final String SMTP_PASS = "mail-pass";

    private final Wiser mailServer = startMailServer(HOSTNAME, SMTP_USER, SMTP_PASS);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "config.mail.host=" + HOSTNAME,
                    "config.mail.port=" + mailServer.getServer().getPort(),
                    "config.mail.from=" + SENDER,
                    "config.mail.username=" + SMTP_USER,
                    "config.mail.password=" + SMTP_PASS,
                    "config.mail.tls=false"
            )
            .build();

    private Path config;
    private Path projectDir;

    @Before
    public void setUp()
            throws Exception
    {
        config = folder.newFile().toPath();

        projectDir = folder.getRoot().toPath().resolve("error");
        Files.createDirectory(projectDir);

        copyResource("acceptance/error_server_mail/error.dig", projectDir.resolve("error.dig"));
        copyResource("acceptance/error_server_mail/fail.sql", projectDir.resolve("fail.sql"));
        copyResource("acceptance/error_server_mail/alert.txt", projectDir.resolve("alert.txt"));
    }

    @After
    public void tearDown()
            throws Exception
    {
        mailServer.stop();
    }

    @Test
    public void verifyMailIsSentWhenWorkflowFails()
            throws Exception
    {
        DigdagClient client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        // Push the project
        CommandStatus pushStatus = main("push",
                "error",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "error", "error",
                "--session", LOCAL_SESSION_TIME);
        assertThat(startStatus.code(), is(0));
        Id attemptId = TestUtils.getAttemptId(startStatus);

        // Wait for the attempt to fail
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 30; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(attempt.getSuccess(), is(false));

        // Wait for mail to be delivered
        for (int i = 0; i < 30; i++) {
            if (mailServer.getMessages().size() > 0) {
                break;
            }
            Thread.sleep(1000);
        }

        // Verify that the mail was correctly delivered
        assertThat(mailServer.getMessages().size(), is(1));
        WiserMessage message = mailServer.getMessages().get(0);
        assertThat(message.getEnvelopeSender(), is(SENDER));
        assertThat(message.getEnvelopeReceiver(), is(RECEIVER));
        assertThat(message.getMimeMessage().getContent(), is("alert " + SESSION_TIME_ISO + "\r\n"));
    }
}
