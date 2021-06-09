package acceptance.td;

import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import org.littleshoot.proxy.HttpProxyServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static acceptance.td.Secrets.TD_API_KEY;
import static acceptance.td.Secrets.secretsServerConfiguration;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.pushProject;

public class TdForEachIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    private Path config;
    private Path projectDir;
    private Path outDir;
    private String projectName;
    private Id projectId;

    private HttpProxyServer proxyServer;

    private DigdagClient digdagClient;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(TD_API_KEY, not(isEmptyOrNullString()));

        proxyServer = TestUtils.startRequestFailingProxy(5);

        server = TemporaryDigdagServer.builder()
                .configuration(secretsServerConfiguration())
                .configuration("config.td.wait.min_poll_interval = 5s")
                .configuration(
                        "params.td.use_ssl = true",
                        "params.td.proxy.enabled = true",
                        "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                        "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
                )
                .build();
        server.start();

        projectDir = folder.newFolder().toPath().toAbsolutePath();
        outDir = folder.newFolder().toPath().toAbsolutePath();

        createProject(projectDir);
        projectName = projectDir.getFileName().toString();
        projectId = pushProject(server.endpoint(), projectDir, projectName);

        digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "td.apikey", TD_API_KEY);
    }

    @Test
    public void testTdForEach()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/td/td_for_each/td_for_each.dig");
        Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_for_each", ImmutableMap.of("outdir", outDir.toString()));

        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));

        // Check that the expected files were created
        assertThat(Files.exists(outDir.resolve("out-a1-b1")), is(true));
        assertThat(Files.exists(outDir.resolve("out-a2-b2")), is(true));
    }
}
