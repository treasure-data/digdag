package acceptance;

import io.digdag.client.DigdagClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class ServerDisableApiIT
{
    private Path config;
    private Path projectDir;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .inProcess(false)
            .configuration(
                    "server.api.disabled = GET_/api/attempts,PUT_/api/projects"
                    )
            .build();

    private Socket socket;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(30*1000);
        socket.connect(new InetSocketAddress(server.host(), server.port()));
    }

    @After
    public void close()
            throws Exception
    {
        socket.close();
    }

    @Test
    public void checkDisabled()
            throws Exception
    {
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/server_disable_api/echo1.dig", projectDir.resolve("echo1.dig"));

        //API is disabled, so push will failed with 404 Not Found
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.code(), is(1));
            assertThat(pushStatus.errUtf8(), containsString("404"));
        }
    }
}
