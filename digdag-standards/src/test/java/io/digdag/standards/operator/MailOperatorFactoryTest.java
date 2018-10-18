package io.digdag.standards.operator;

import com.google.common.io.Resources;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.subethamail.wiser.Wiser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class MailOperatorFactoryTest
{

    private static Wiser wiser;

    final static String hostName = "localhost";
    final static int port = 2500;

    private MailOperatorFactory factory;
    private Path tempPath;
    private Config config;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void smtpSetup()
    {
        wiser = new Wiser();
        wiser.setPort(port);
        wiser.setHostname(hostName);
        wiser.start();
    }

    @AfterClass
    public static void shutdownSmtp()
    {
        wiser.stop();
    }

    @Before
    public void createInstance() throws IOException
    {
        this.factory = newOperatorFactory(MailOperatorFactory.class);
        this.tempPath = folder.getRoot().toPath();

        String body = Resources.toString(getClass().getResource("mail_body.txt"),UTF_8);
        Files.write(Paths.get(this.tempPath.toString(),"mail_body.txt"),body.getBytes());
    }

    @Test
    public void sendEmail()
    {
        Config config = newConfig()
                .set("host","localhost")
                .set("port",2500)
                .set("subject","test")
                .set("_command","mail_body.txt")
                .set("timezone","Asia/Tokyo")
                .set("to","bob@example.com")
                .set("from","alice@example.com");
        try {
            String hoge = Resources.toString(getClass().getResource("mail_body.txt"),UTF_8);
            System.out.println(hoge);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(tempPath);
        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(config)));
        op.run();
        assertEquals(true,false);
    }
}