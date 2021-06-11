package acceptance;

import acceptance.td.Secrets;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
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
import java.util.Base64;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.startMailServer;

public class ServerModeMailIT
{
    private static final String SENDER = "alert@digdag.io";
    private static final String RECEIVER = "test@digdag.io";
    private static final String LOCAL_SESSION_TIME = "2016-01-02 03:04:05";
    private static final String SESSION_TIME_ISO = "2016-01-02T03:04:05+00:00";
    private static final String HOSTNAME = "127.0.0.1";
    private static final String USERNAME = "test-smtp-user";
    private static final String PASSWORD = "test-smtp-pass";

    private Wiser mailServer = startMailServer(HOSTNAME, USERNAME, PASSWORD);

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "params.mail.host=" + HOSTNAME,
                    "params.mail.port=" + mailServer.getServer().getPort(),
                    "params.mail.from=" + SENDER,
                    "params.mail.username=" + USERNAME,
                    "params.mail.tls=false"
            )
            .withRandomSecretEncryptionKey()
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

    @After
    public void tearDown()
            throws Exception
    {
        mailServer.stop();
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

        Id projectId = TestUtils.getProjectId(pushStatus);
        client.setProjectSecret(projectId, "mail.password", PASSWORD);

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
}
