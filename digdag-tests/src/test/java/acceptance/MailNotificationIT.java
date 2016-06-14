package acceptance;

import com.google.common.base.Joiner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import static acceptance.TestUtils.attemptSuccess;
import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.expect;
import static acceptance.TestUtils.getAttemptId;
import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MailNotificationIT
{
    private static final String PROJECT_NAME = "notification";
    private static final String WORKFLOW_NAME = "notification-test-wf";
    private static final String HOSTNAME = "127.0.0.1";
    private static final String SENDER = "digdag@foo.bar";
    private static final String RECEIVER = "alert@foo.bar";

    private final int port = TestUtils.findFreePort();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(Joiner.on('\n').join(
                    "notification.type = mail",
                    "notification.mail.to = " + RECEIVER,
                    "notification.mail.from = " + SENDER,
                    "notification.mail.host= " + HOSTNAME,
                    "notification.mail.port=" + port,
                    "notification.mail.username=mail-user",
                    "notification.mail.password=mail-pass",
                    "notification.mail.tls=false"))
            .build();

    private Path config;
    private Path projectDir;
    private Wiser mailServer;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        TestUtils.createProject(projectDir);

        mailServer = new Wiser();
        mailServer.setHostname(HOSTNAME);
        mailServer.setPort(port);
        mailServer.start();
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (mailServer != null) {
            mailServer.stop();
        }
    }

    @Test
    public void testSlaDurationAlertMail()
            throws Exception
    {
        pushAndStart("acceptance/sla/duration_alert_default.dig");

        // Wait for mail to be delivered
        expect(Duration.ofSeconds(30), () -> mailServer.getMessages().size() > 0);

        // Verify that the mail was correctly delivered
        assertThat(mailServer.getMessages().size(), is(1));
        WiserMessage message = mailServer.getMessages().get(0);
        assertThat(message.getEnvelopeSender(), is(SENDER));
        assertThat(message.getEnvelopeReceiver(), is(RECEIVER));

        String content = (String) message.getMimeMessage().getContent();

        assertThat(content, containsString("Digdag Notification"));
        assertThat(content, containsString("SLA violation"));
    }

    @Test
    public void testSessionFailureAlertMail()
            throws Exception
    {
        pushAndStart("acceptance/notification/fail.dig");

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

        String content = (String) message.getMimeMessage().getContent();

        assertThat(content, containsString("Digdag Notification"));
        assertThat(content, containsString("Workflow session attempt failed"));
    }

    @Test
    public void testSessionSuccess()
            throws Exception
    {
        long attemptId = pushAndStart("acceptance/notification/success.dig");
        expect(Duration.ofSeconds(30), attemptSuccess(server.endpoint(), attemptId));

        Thread.sleep(5000);

        // Verify that no mail was delivered
        assertThat(mailServer.getMessages().size(), is(0));
    }

    private long pushAndStart(String workflow)
            throws IOException
    {
        copyResource(workflow, projectDir.resolve(WORKFLOW_NAME + ".dig"));

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
