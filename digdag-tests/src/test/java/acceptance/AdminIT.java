package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptRequest;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.config.Config;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class AdminIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration("server.admin.port = 0")
            .configuration("server.authenticator.type = basic")
            .configuration("server.authenticator.basic.username = user1")
            .configuration("server.authenticator.basic.password = pass1")
            .configuration("server.authenticator.basic.admin = true")
            .build();

    private Path config;
    private Path projectDir;
    private DigdagClient client;
    private DigdagClient adminClient;

    private static final String basicAuthHeaderValue = "Basic dXNlcjE6cGFzczE="; // user1:pass1
    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.newFolder().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .header("Authorization",  basicAuthHeaderValue)
                .build();

        adminClient = DigdagClient.builder()
                .host(server.host())
                .port(server.adminPort())
                .header("Authorization", basicAuthHeaderValue)
                .build();
    }

    @Test
    public void testGetUserInfo()
            throws Exception
    {
        TestUtils.addWorkflow(projectDir, "acceptance/basic.dig");
        String projectName = projectDir.getFileName().toString();
        Id projectId = TestUtils.pushProject(server.endpoint(), projectDir, projectName, Arrays.asList("--basic-auth", "user1:pass1"));
        RestWorkflowDefinition wf = client.getWorkflowDefinition(projectId, "basic");
        RestSessionAttemptRequest startReq = RestSessionAttemptRequest.builder()
                .workflowId(wf.getId())
                .sessionTime(Instant.now())
                .params(TestUtils.configFactory().create())
                .build();
        RestSessionAttempt attempt = client.startSessionAttempt(startReq);
        Id attemptId = attempt.getId();
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
