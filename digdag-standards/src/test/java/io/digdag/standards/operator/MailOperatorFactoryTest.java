package io.digdag.standards.operator;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.icegreen.greenmail.server.AbstractServer;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.Retriever;
import com.icegreen.greenmail.util.ServerSetup;
import com.icegreen.greenmail.util.ServerSetupTest;
import com.sun.mail.util.MailConnectException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.util.DurationParam;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.mail.Message;
import javax.mail.MessagingException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MailOperatorFactoryTest
{

    private static GreenMail greenMail;
    private static int smtpPort;

    private MailOperatorFactory factory;
    private Path tempPath;

    private Config mailConfig;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void smtpSetup()
    {
        ServerSetup servers[] = {ServerSetupTest.SMTP, ServerSetupTest.POP3};
        greenMail = new GreenMail(servers);
        greenMail.start();
        smtpPort = greenMail.getSmtp().getPort();
    }

    @AfterClass
    public static void shutdownSmtp()
    {
        greenMail.stop();
    }

    @Before
    public void createInstance()
            throws IOException
    {
        this.factory = newOperatorFactory(MailOperatorFactory.class);
        this.tempPath = folder.getRoot().toPath();

        String body = Resources.toString(getClass().getResource("mail_body.txt"), UTF_8);
        Files.write(Paths.get(this.tempPath.toString(), "mail_body.txt"), body.getBytes());

        this.mailConfig = newConfig()
                .set("host", "localhost")
                .set("port", smtpPort)
                .set("subject", "test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com");
    }

    @After
    public void resetGreenMail()
    {
        greenMail.reset();
    }

    @Test
    public void sendSingleToEmail()
    {
        mailConfig.set("to", "bob@example.com");

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        receiveCheck("bob@example.com", 1);
    }

    @Test
    public void sendMultipleToEmail()
    {
        mailConfig.set("to", ImmutableList.of("bob@example.com", "charlie@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        receiveCheck("bob@example.com", 1);
        receiveCheck("charlie@example.com", 1);
    }

    @Test
    public void sendSingleToAndCcBccEmail()
    {
        mailConfig.set("to", "bob@example.com")
                .set("bcc", ImmutableList.of("charlie@example.com"))
                .set("cc", ImmutableList.of("david@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        greenMail.waitForIncomingEmail(5000, 2);

        receiveCheck("bob@example.com", 1);
        receiveCheck("charlie@example.com", 1);
        receiveCheck("david@example.com", 1);
    }

    @Test
    public void sendSingleToCcEmail()
    {
        mailConfig.set("to", "bob@example.com")
                .set("cc", ImmutableList.of("charlie@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        greenMail.waitForIncomingEmail(5000, 2);

        receiveCheck("bob@example.com", 1);
        receiveCheck("charlie@example.com", 1);
    }

    @Test
    public void sendSingleToBccEmail()
    {
        mailConfig.set("to", "bob@example.com")
                .set("bcc", ImmutableList.of("charlie@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        greenMail.waitForIncomingEmail(5000, 2);

        receiveCheck("bob@example.com", 1);
        receiveCheck("charlie@example.com", 1);
    }

    @Test
    public void errorCheckNoDestination()
    {
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckRequireTo()
    {
        mailConfig.set("cc", ImmutableList.of("bob@example.com"));
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckRequireTo2()
    {
        mailConfig.set("bcc", ImmutableList.of("bob@example.com"));
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoHost()
    {
        Config config = newConfig()
//                .set("host","localhost")
                .set("port", smtpPort)
                .set("subject", "test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com")
                .set("to", "bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (TaskExecutionException ignore) {
        }
    }

    @Test
    public void errorCheckNoPort()
    {
        Config config = newConfig()
                .set("host", "localhost")
//                .set("port",smtpPort)
                .set("subject", "test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com")
                .set("to", "bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoSubject()
    {
        Config config = newConfig()
                .set("host", "localhost")
                .set("port", smtpPort)
//                .set("subject","test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com")
                .set("to", "bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoCommand()
    {
        Config config = newConfig()
                .set("host", "localhost")
                .set("port", smtpPort)
                .set("subject", "test")
//                .set("_command","mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com")
                .set("to", "bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoFrom()
    {
        Config config = newConfig()
                .set("host", "localhost")
                .set("port", smtpPort)
                .set("subject", "test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
//                .set("from","alice@example.com")
                .set("to", "bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckStringCc()
    {
        mailConfig.set("to", "bob@example.com")
                .set("cc", "charlie@example.com");
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckStringBcc()
    {
        mailConfig.set("to", "bob@example.com")
                .set("bcc", "charlie@example.com");
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        }
        catch (ConfigException ignore) {
        }
    }

    @Test
    public void connectionTimeout()
    {
        Config config = newConfig()
                // 192.0.2.0/24 shouldn't be reached since it's used only for test (RFC-1166)
                .set("host", "192.0.2.0")
                .set("port", 25)
                .set("subject", "test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com")
                .set("to", "bob@example.com")
                .set("connect_timeout", "5s");

        assertWithDuration(() -> {
                    try {
                        Operator op = factory.newOperator(newContext(
                                tempPath,
                                newTaskRequest().withConfig(config)));
                        op.run();
                        fail();
                    }
                    catch (TaskExecutionException e) {
                        Throwable cause = e.getCause();
                        assertTrue(cause instanceof MailConnectException);
                    }
                },
                // Specified connection timeout is 5 seconds
                Duration.ofSeconds(5), Duration.ofSeconds(5 * 3)
        );
    }

    @Test
    public void socketTimeout()
            throws IOException
    {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(0));
        ExecutorService executorService = serverSocketConsumer(serverSocketChannel);

        Config config = newConfig()
                .set("host", "localhost")
                .set("port", serverSocketChannel.socket().getLocalPort())
                .set("subject", "test")
                .set("_command", "mail_body.txt")
                .set("timezone", "Asia/Tokyo")
                .set("from", "alice@example.com")
                .set("to", "bob@example.com")
                .set("socket_timeout", "5s");

        try {
            assertWithDuration(() -> {
                        try {
                            Operator op = factory.newOperator(newContext(
                                    tempPath,
                                    newTaskRequest().withConfig(config)));
                            op.run();
                            fail();
                        } catch (TaskExecutionException e) {
                            Throwable cause = e.getCause();
                            assertTrue(cause instanceof MessagingException);
                        }
                    },
                    // Specified socket timeout is 5 seconds
                    Duration.ofSeconds(5), Duration.ofSeconds(5 * 3)
            );
        }
        finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void systemSmtpConfig()
    {
        Config systemConfig = newConfig()
                .set("config.mail.host", "mail.digdag.io")
                .set("config.mail.port", 2525)
                .set("config.mail.tls", false)
                .set("config.mail.ssl", true)
                .set("config.mail.debug", true)
                .set("config.mail.username", "hello")
                .set("config.mail.password", "world1234")
                .set("config.mail.connect_timeout", "42s")
                .set("config.mail.socket_timeout", "7m");

        MailOperatorFactory.SmtpConfig smtpConfig = MailOperatorFactory.systemSmtpConfig(systemConfig).get();
        assertEquals("mail.digdag.io", smtpConfig.host());
        assertEquals(2525, smtpConfig.port());
        assertFalse(smtpConfig.startTls());
        assertTrue(smtpConfig.ssl());
        assertTrue(smtpConfig.debug());
        assertEquals("hello", smtpConfig.username().get());
        assertEquals("world1234", smtpConfig.password().get());
        assertEquals(DurationParam.of(Duration.ofSeconds(42)), smtpConfig.connectionTimeout().get());
        assertEquals(DurationParam.of(Duration.ofMinutes(7)), smtpConfig.socketTimeout().get());
    }

    @Test
    public void defaultSystemSmtpConfig()
    {
        Config systemConfig = newConfig()
                .set("config.mail.host", "mail.digdag.io")
                .set("config.mail.port", 2525);

        MailOperatorFactory.SmtpConfig smtpConfig = MailOperatorFactory.systemSmtpConfig(systemConfig).get();
        assertEquals("mail.digdag.io", smtpConfig.host());
        assertEquals(2525, smtpConfig.port());
        assertTrue(smtpConfig.startTls());
        assertFalse(smtpConfig.ssl());
        assertFalse(smtpConfig.debug());
        assertFalse(smtpConfig.username().isPresent());
        assertFalse(smtpConfig.password().isPresent());
        assertFalse(smtpConfig.connectionTimeout().isPresent());
        assertFalse(smtpConfig.socketTimeout().isPresent());
    }

    @Test
    public void userSmtpConfig()
    {
        SecretProvider secretProvider = mock(SecretProvider.class);
        doReturn(Optional.of("hello")).when(secretProvider).getSecretOptional(eq("username"));
        doReturn(Optional.of("world1234")).when(secretProvider).getSecretOptional(eq("password"));
        doReturn(Optional.of("mail.digdag.io")).when(secretProvider).getSecretOptional(eq("host"));
        doReturn(Optional.of("2525")).when(secretProvider).getSecretOptional(eq("port"));
        doReturn(Optional.of(false)).when(secretProvider).getSecretOptional(eq("tls"));
        doReturn(Optional.of(true)).when(secretProvider).getSecretOptional(eq("ssl"));
        doReturn(Optional.of(true)).when(secretProvider).getSecretOptional(eq("debug"));

        Config params = newConfig()
                .set("connect_timeout", "42s")
                .set("socket_timeout", "7m");

        MailOperatorFactory.SmtpConfig smtpConfig = MailOperatorFactory.userSmtpConfig(secretProvider, params).get();
        assertEquals("mail.digdag.io", smtpConfig.host());
        assertEquals(2525, smtpConfig.port());
        assertFalse(smtpConfig.startTls());
        assertTrue(smtpConfig.ssl());
        assertTrue(smtpConfig.debug());
        assertEquals("hello", smtpConfig.username().get());
        assertEquals("world1234", smtpConfig.password().get());
        assertEquals(DurationParam.of(Duration.ofSeconds(42)), smtpConfig.connectionTimeout().get());
        assertEquals(DurationParam.of(Duration.ofMinutes(7)), smtpConfig.socketTimeout().get());
    }

    private ExecutorService serverSocketConsumer(ServerSocketChannel serverSocketChannel)
    {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            while (!executorService.isShutdown()) {
                try {
                    SocketChannel channel = serverSocketChannel.accept();
                    ByteBuffer buffer = ByteBuffer.allocate(512);
                    while (!executorService.isShutdown()) {
                        channel.read(buffer);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return executorService;
    }

    private void assertWithDuration(Runnable task, Duration min, Duration max)
    {
        Instant start = Instant.now();
        task.run();
        Duration duration = Duration.between(start, Instant.now());
        assertThat(duration, greaterThanOrEqualTo(min));
        assertThat(duration, lessThanOrEqualTo(max));
    }

    private void receiveCheck(String user, int size)
    {
        AbstractServer server = greenMail.getPop3();
        Retriever retriever = new Retriever(server);

        Message[] messages = retriever.getMessages(user);
        assertEquals(size, messages.length);
    }
}
