package acceptance;

import com.google.common.base.Joiner;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

import java.nio.file.Files;
import java.nio.file.Path;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.getAttemptId;
import static acceptance.TestUtils.main;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ServerModeHiddenMailConfigIT
{
    private static final String SENDER = "alert@digdag.io";
    private static final String RECEIVER = "test@digdag.io";
    private static final String LOCAL_SESSION_TIME = "2016-01-02 03:04:05";
    private static final String SESSION_TIME_ISO = "2016-01-02T03:04:05+00:00";
    private static final String HOSTNAME = "127.0.0.1";

    private final int port = TestUtils.findFreePort();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(Joiner.on("\n").join(asList(
                    "config.mail.host=" + HOSTNAME,
                    "config.mail.port=" + port,
                    "config.mail.from=" + SENDER,
                    "config.mail.username=mail-user",
                    "config.mail.password=mail-pass",
                    "config.mail.tls=false"
            )))
            .build();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void mailConfigInFile()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath();
        Path workflowFile = projectDir.resolve("mail_config.dig");
        Path configDir = folder.newFolder().toPath();
        Path config = configDir.resolve("config");
        Files.createFile(config);

        // Start mail server
        Wiser mailServer;
        mailServer = new Wiser();
        mailServer.setHostname(HOSTNAME);
        mailServer.setPort(port);
        mailServer.start();

        copyResource("acceptance/mail_config/mail_config.dig", workflowFile);
        copyResource("acceptance/mail_config/mail_body.txt", projectDir.resolve("mail_body.txt"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "mail_config",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "mail_config", "mail_config",
                "--session", LOCAL_SESSION_TIME);
        assertThat(startStatus.code(), is(0));

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
        assertThat(message.getMimeMessage().getContent(), is("hello world " + SESSION_TIME_ISO + "\r\n"));
    }

    @Test
    public void verifyMailPasswordIsNotAccessibleToUser()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath();
        Path workflowFile = projectDir.resolve("mail_config.dig");
        Path configDir = folder.newFolder().toPath();
        Path config = configDir.resolve("config");
        Files.createFile(config);

        copyResource("acceptance/mail_config/mail_config.dig", workflowFile);
        copyResource("acceptance/mail_config/evil_mail_body.txt", projectDir.resolve("mail_body.txt"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "mail_config",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.code(), is(0));

        // Start the workflow
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "mail_config", "mail_config",
                "--session", LOCAL_SESSION_TIME);
        assertThat(startStatus.code(), is(0));

        long attemptId = getAttemptId(startStatus);

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
        String logs = TestUtils.getAttemptLogs(client, attemptId);
        System.out.println(logs);

        // XXX (dano): very hacky but good enough for now
        assertThat(logs, containsString("Configuration error at task +mail_config+foo"));
        assertThat(logs, containsString("Failed to evaluate JavaScript code: ${config.mail.password}"));
        assertThat(logs, containsString("\"config\" is not defined"));
    }
}
