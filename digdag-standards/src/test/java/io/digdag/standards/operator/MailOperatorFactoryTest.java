package io.digdag.standards.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.icegreen.greenmail.server.AbstractServer;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.Retriever;
import com.icegreen.greenmail.util.ServerSetup;
import com.icegreen.greenmail.util.ServerSetupTest;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskExecutionException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.mail.Message;
import javax.mail.internet.InternetAddress;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    public void createInstance() throws IOException
    {
        this.factory = newOperatorFactory(MailOperatorFactory.class);
        this.tempPath = folder.getRoot().toPath();

        String body = Resources.toString(getClass().getResource("mail_body.txt"),UTF_8);
        Files.write(Paths.get(this.tempPath.toString(),"mail_body.txt"),body.getBytes());

        this.mailConfig = newConfig()
                .set("host","localhost")
                .set("port",smtpPort)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("from","alice@example.com");

    }

    @After
    public void resetGreenMail()
    {
        greenMail.reset();
    }

    @Test
    public void sendSingleToEmail()
    {
        mailConfig.set("to","bob@example.com");

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        receiveCheck("bob@example.com",1);
    }

    @Test
    public void sendMultipleToEmail()
    {
        mailConfig.set("to",ImmutableList.of("bob@example.com","charlie@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        receiveCheck("bob@example.com",1);
        receiveCheck("charlie@example.com",1);
     }

    @Test
    public void sendSingleToAndCcBccEmail()
    {
        mailConfig.set("to","bob@example.com")
                .set("bcc", ImmutableList.of("charlie@example.com"))
                .set("cc",ImmutableList.of("david@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        greenMail.waitForIncomingEmail(5000,2);

        receiveCheck("bob@example.com",1);
        receiveCheck("charlie@example.com",1);
        receiveCheck("david@example.com",1);
    }

    @Test
    public void sendSingleToCcEmail()
    {
        mailConfig.set("to","bob@example.com")
                .set("cc", ImmutableList.of("charlie@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        greenMail.waitForIncomingEmail(5000,2);

        receiveCheck("bob@example.com",1);
        receiveCheck("charlie@example.com",1);
    }

    @Test
    public void sendSingleToBccEmail()
    {
        mailConfig.set("to","bob@example.com")
                .set("bcc", ImmutableList.of("charlie@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(mailConfig)));
        op.run();
        greenMail.waitForIncomingEmail(5000,2);

        receiveCheck("bob@example.com",1);
        receiveCheck("charlie@example.com",1);
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
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckRequireTo()
    {
        mailConfig.set("cc",ImmutableList.of("bob@example.com"));
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckRequireTo2()
    {
        mailConfig.set("bcc",ImmutableList.of("bob@example.com"));
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Ignore
    public void errorCheckNoHost()
    {
        Config config = newConfig()
//                .set("host","localhost")
                .set("port",smtpPort)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("from","alice@example.com")
                .set("to","bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoPort()
    {
        Config config = newConfig()
                .set("host","localhost")
//                .set("port",smtpPort)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("from","alice@example.com")
                .set("to","bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoSubject()
    {
        Config config = newConfig()
                .set("host","localhost")
                .set("port",smtpPort)
//                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("from","alice@example.com")
                .set("to","bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoCommand()
    {
        Config config = newConfig()
                .set("host","localhost")
                .set("port",smtpPort)
                .set("subject","test")
//                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("from","alice@example.com")
                .set("to","bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckNoFrom()
    {
        Config config = newConfig()
                .set("host","localhost")
                .set("port",smtpPort)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
//                .set("from","alice@example.com")
                .set("to","bob@example.com");

        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(config)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckStringCc()
    {
        mailConfig.set("to","bob@example.com")
                .set("cc","charlie@example.com");
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    @Test
    public void errorCheckStringBcc()
    {
        mailConfig.set("to","bob@example.com")
                .set("bcc","charlie@example.com");
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(mailConfig)));
            op.run();
            fail("should be thrown Exception.");
        } catch (ConfigException ignore) {
        }
    }

    private void receiveCheck(String user, int size)
    {
        AbstractServer server = greenMail.getPop3();
        Retriever retriever = new Retriever(server);

        Message[] messages = retriever.getMessages(user);
        assertEquals(size,messages.length);
    }
}
