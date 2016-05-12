package acceptance;

import com.google.common.base.Joiner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LocalModeMailIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void mailConfigInFile()
            throws Exception
    {
        Path projectDir = folder.newFolder().toPath();
        Path configDir = folder.newFolder().toPath();

        String sender = "alert@digdag.io";
        String receiver = "test@digdag.io";
        String localSessionTime = "2016-01-02 03:04:05";
        String sessionTimeISO = "2016-01-02T03:04:05+00:00";

        // Start mail server
        Wiser mailServer;
        mailServer = new Wiser();
        mailServer.setHostname("127.0.0.1");
        mailServer.setPort(4711);
        mailServer.start();

        // Add mail config to digdag configuration file
        copyResource("acceptance/mail_config/mail_config.yml", projectDir.resolve("mail_config.yml"));
        copyResource("acceptance/mail_config/mail_body.txt", projectDir.resolve("mail_body.txt"));
        String config = Joiner.on("\n").join(asList(
                "params.mail.host=127.0.0.1",
                "params.mail.port=4711",
                "params.mail.from=" + sender,
                "params.mail.username=mail-user",
                "params.mail.password=mail-pass",
                "params.mail.tls=false"
        ));
        Path configFile = configDir.resolve("config");
        Files.write(configFile, config.getBytes(UTF_8));

        // Run a workflow that sends a mail
        main("run",
                "-c", configFile.toString(),
                "-o", projectDir.toString(),
                "-f", projectDir.resolve("mail_config.yml").toString(),
                "--session", localSessionTime);

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
        assertThat(message.getEnvelopeSender(), is(sender));
        assertThat(message.getEnvelopeReceiver(), is(receiver));
        assertThat(message.getMimeMessage().getContent(), is("hello world " + sessionTimeISO + "\r\n"));
    }
}
