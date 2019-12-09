package acceptance;

import io.netty.handler.codec.http.FullHttpRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class CliDownloadIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;
    private HttpProxyServer proxyServer;

    private Map<String, String> env;
    private Path config;
    private Path projectDir;

    private List<FullHttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void setUp()
            throws Exception
    {
        env = new HashMap<>();
        proxyServer = TestUtils.startRequestTrackingProxy(requests);
        server = TemporaryDigdagServer.builder()
                    .withRandomSecretEncryptionKey()
                    .build();
        server.start();
        projectDir = folder.getRoot().toPath().resolve("foobar");
        String proxyUrl = "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort();
        env.put("http_proxy", proxyUrl);

        config = folder.newFile().toPath();
    }

    @Test
    public void disableDirectDownload()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "test_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint()
            );
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // --disable-direct-download
        {
            requests.clear();
            CommandStatus status = main(env,
                    "download",
                    "test_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-o", folder.getRoot().toPath().resolve("test2").toString(),
                    "--disable-direct-download"
            );
            assertThat(status.errUtf8(), status.code(), is(0));
            boolean match = false;
            for (FullHttpRequest req : requests) {
                if (req.uri().matches(".*/api/projects/.*/archive.*")) {
                    match = true;
                    assertThat("direct_download= must be set.", req.uri().matches(".*direct_download=.*"));
                }
            }
            assertThat("No record", match);
        }

        //Neither --disable-direct-download nor client.http.disable_direct_download=true
        {
            requests.clear();
            CommandStatus status = main(env,
                    "download",
                    "test_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-o", folder.getRoot().toPath().resolve("test1").toString()
            );
            assertThat(status.errUtf8(), status.code(), is(0));
            boolean match = false;
            for (FullHttpRequest req : requests) {
                if (req.uri().matches(".*/api/projects/.*/archive.*")) {
                    match = true;
                    assertThat("direct_download= must not be set.",
                            !req.uri().matches(".*direct_download=.*"));
                }
            }
            assertThat("No record", match);
        }

        // client.http.disable_direct_download=true
        {
            requests.clear();
            Files.write(config, Arrays.asList("client.http.disable_direct_download=true"));
            CommandStatus status = main(env,
                    "download",
                    "test_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-o", folder.getRoot().toPath().resolve("test3").toString()
            );
            assertThat(status.errUtf8(), status.code(), is(0));
            boolean match = false;
            for (FullHttpRequest req : requests) {
                if (req.uri().matches(".*/api/projects/.*/archive.*")) {
                    match = true;
                    assertThat("direct_download= must be set.",req.uri().matches(".*direct_download=.*"));
                }
            }
            assertThat("No record", match);
        }
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
        if (server != null) {
            server.close();
            server = null;
        }
    }
}
