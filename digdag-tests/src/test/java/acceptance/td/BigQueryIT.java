package acceptance.td;

import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpProxyServer;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.pushProject;

public class BigQueryIT
{
    private static final String GCP_CREDENTIAL = System.getenv().getOrDefault("GCP_CREDENTIAL", "");

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    private Path projectDir;
    private String projectName;
    private int projectId;

    private Path outfile;

    private DigdagClient digdagClient;

    private HttpProxyServer proxyServer;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(GCP_CREDENTIAL, not(isEmptyOrNullString()));

        proxyServer = TestUtils.startRequestFailingProxy(3);

        server = TemporaryDigdagServer.builder()
                .environment(ImmutableMap.of(
                        "https_proxy", "http://" + proxyServer.getListenAddress().getHostString() + ":" + proxyServer.getListenAddress().getPort())
                )
                .withRandomSecretEncryptionKey()
                .build();
        server.start();

        projectDir = folder.getRoot().toPath();
        createProject(projectDir);
        projectName = projectDir.getFileName().toString();
        projectId = pushProject(server.endpoint(), projectDir, projectName);

        outfile = folder.newFolder().toPath().resolve("outfile");

        digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "gcp.credential", GCP_CREDENTIAL);
    }

    @After
    public void tearDownDigdagServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @After
    public void tearDownProxyServer()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
    }

    @Test
    public void testQuery()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/bigquery/query.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "query", ImmutableMap.of("outfile", outfile.toString()));
        expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), attemptId));
        assertThat(Files.exists(outfile), is(true));
    }
}
