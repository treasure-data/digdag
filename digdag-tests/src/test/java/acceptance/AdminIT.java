package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.config.Config;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;

import java.nio.file.Path;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class AdminIT
{
    @ClassRule
    public static TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration("server.admin.port = 0")
            .build();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;
    private DigdagClient client;
    private DigdagClient adminClient;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.newFolder().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        adminClient = DigdagClient.builder()
                .host(server.host())
                .port(server.adminPort())
                .build();
    }

    @After
    public void tearDown()
    {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        if (folder != null) {
            folder.delete();
        }
    }

    @Test
    public void testGetUserInfo()
            throws Exception
    {
        TestUtils.addWorkflow(projectDir, "acceptance/basic.dig");
        Id attemptId = TestUtils.pushAndStart(server.endpoint(), projectDir, "basic");

        Config userinfo = adminClient.adminGetAttemptUserInfo(attemptId);
        assertThat(userinfo, is(not(Matchers.nullValue())));

        // Verify that the admin endpoint is not reachable on the normal api port
        try {
            client.adminGetAttemptUserInfo(attemptId);
            fail();
        }
        catch (NotFoundException e) {
        }
    }
}
