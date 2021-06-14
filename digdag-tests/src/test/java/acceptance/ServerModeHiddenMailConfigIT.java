package acceptance;

import com.google.common.collect.ImmutableList;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.subethamail.smtp.AuthenticationHandler;
import org.subethamail.smtp.AuthenticationHandlerFactory;
import org.subethamail.smtp.RejectException;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static com.google.common.primitives.Bytes.concat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.addResource;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.createProject;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static utils.TestUtils.pushProject;
import static utils.TestUtils.startMailServer;

public class ServerModeHiddenMailConfigIT
{
    private static final String SENDER = "alert@digdag.io";
    private static final String RECEIVER = "test@digdag.io";
    private static final String LOCAL_SESSION_TIME = "2016-01-02 03:04:05";
    private static final String SESSION_TIME_ISO = "2016-01-02T03:04:05+00:00";
    private static final String HOSTNAME = "127.0.0.1";
    private static final String USER_SMTP_USER = "user-mail-user";
    private static final String SMTP_USER = "mail-user";
    private static final String SMTP_PASS = "mail-pass";

    private final Wiser mailServer = startMailServer(HOSTNAME, SMTP_USER, SMTP_PASS);

    private final BlockingQueue<String> auth = new LinkedBlockingDeque<>();
    private final Wiser userMailServer = startMailServer(HOSTNAME, new AuthenticationHandlerFactory() {
        @Override
        public List<String> getAuthenticationMechanisms()
        {
            return ImmutableList.of("PLAIN");
        }

        @Override
        public AuthenticationHandler create()
        {
            return new AuthenticationHandler() {
                @Override
                public String auth(String clientInput)
                        throws RejectException
                {
                    auth.add(clientInput);
                    throw new RejectException();
                }

                @Override
                public Object getIdentity()
                {
                    throw new AssertionError();
                }
            };
        }
    });

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
        if (client != null) {
            client.close();
            client = null;
        }
        mailServer.stop();
        userMailServer.stop();
    }

    @Test
    public void verifySystemPasswordIsNotSentToUserSmtpServer()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath().resolve("mail_config");
        Path configDir = folder.newFolder().toPath();
        Path config = configDir.resolve("config");
        Files.createFile(config);

        createProject(projectDir);
        addWorkflow(projectDir, "acceptance/mail_config/mail_config.dig");
        addResource(projectDir, "acceptance/mail_config/mail_body.txt");
        pushProject(server.endpoint(), projectDir, "mail_config");

        // Start the workflow
        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "mail_config", "mail_config",
                "-p", "mail.tls=false",
                "-p", "mail.host=" + userMailServer.getServer().getHostName(),
                "-p", "mail.port=" + userMailServer.getServer().getPort(),
                "-p", "mail.username=" + USER_SMTP_USER,
                "--session", "now");
        assertThat(startStatus.code(), is(0));

        // Verify that digdag does not send the system smtp password
        String clientInput = auth.take();

        String prefix = "AUTH PLAIN ";
        assertThat(clientInput, Matchers.startsWith(prefix));
        String credentialsBase64 = clientInput.substring(prefix.length());
        byte[] credentials = Base64.getDecoder().decode(credentialsBase64);

        byte[] expectedCredentials = concat(
                USER_SMTP_USER.getBytes(UTF_8),
                new byte[] {0},
                USER_SMTP_USER.getBytes(UTF_8),
                new byte[] {0});

        assertThat(credentials, is(expectedCredentials));
    }

    @Test
    public void mailConfigInFile()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath().resolve("mail_config");
        Path configDir = folder.newFolder().toPath();
        Path config = configDir.resolve("config");
        Files.createFile(config);

        createProject(projectDir);
        addWorkflow(projectDir, "acceptance/mail_config/mail_config.dig");
        addResource(projectDir, "acceptance/mail_config/mail_body.txt");
        pushProject(server.endpoint(), projectDir, "mail_config");

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

        Id attemptId = getAttemptId(startStatus);

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
        assertThat(logs, containsString("Failed to evaluate a variable ${config.mail.password} (ReferenceError: \"config\" is not defined) in mail_body.txt"));
    }
}
