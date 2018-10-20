package io.digdag.standards.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

public class MailOperatorFactoryTest
{

    private static GreenMail greenMail;
    private static int smtpPort;

    private MailOperatorFactory factory;
    private Path tempPath;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void smtpSetup()
    {
        greenMail = new GreenMail(ServerSetupTest.SMTP);
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
    }

    @After
    public void resetGreenMail()
    {
        greenMail.reset();
    }

    @Test
    public void sendEmail()
    {
        Config config = newConfig()
                .set("host","localhost")
                .set("port",smtpPort)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("to","bob@example.com")
                .set("from","alice@example.com");

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(config)));
        op.run();
            assertEquals(1,greenMail.getReceivedMessages().length);
    }

    @Test
    public void sendEmail2()
    {
        Config config = newConfig()
                .set("host","localhost")
                .set("port",smtpPort)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("to","bob@example.com")
                .set("from","alice@example.com")
                .set("bcc", ImmutableList.of("charlie@example.com"))
                .set("cc",ImmutableList.of("david@example.com"));

        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(config)));
        op.run();
        greenMail.waitForIncomingEmail(5000,3);
//        assertEquals(3,greenMail.getReceivedMessages().length);
    }

}