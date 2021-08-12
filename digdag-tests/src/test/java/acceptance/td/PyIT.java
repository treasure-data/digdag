package acceptance.td;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.config.Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.main;

public class PyIT
{
    private static final String ECS_CONFIG = System.getenv("ECS_IT_CONFIG");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TemporaryDigdagServer server;

    private String accessKeyId;
    private String secretAccessKey;
    // AWS ECS
    private String ecsCluster;
    private String ecsLaunchType;
    private String ecsRegion;
    private String ecsSubnets;
    // AWS S3
    private String s3Bucket;
    private String s3Endpoint;

    private Config config;
    private Path configFile;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(ECS_CONFIG, not(isEmptyOrNullString()));

        ObjectMapper objectMapper = DigdagClient.objectMapper();
        config = Config.deserializeFromJackson(objectMapper, objectMapper.readTree(ECS_CONFIG));

        accessKeyId = config.get("access_key_id", String.class);
        secretAccessKey = config.get("secret_access_key", String.class);

        ecsCluster = config.get("ecs_cluster", String.class);
        ecsLaunchType = config.get("ecs_launch_type", String.class);
        ecsRegion = config.get("ecs_region", String.class);
        ecsSubnets = config.get("ecs_subnets", String.class);

        s3Bucket = config.get("s3_bucket", String.class);
        s3Endpoint = config.get("s3_endopint", String.class);

        server = TemporaryDigdagServer.builder()
                .configuration(
                        "agent.command_executor.ecs.name=" + ecsCluster,
                        "agent.command_executor.ecs." + ecsCluster + ".launch_type=" + ecsLaunchType,
                        "agent.command_executor.ecs." + ecsCluster + ".access_key_id=" + accessKeyId,
                        "agent.command_executor.ecs." + ecsCluster + ".secret_access_key=" + secretAccessKey,
                        "agent.command_executor.ecs." + ecsCluster + ".region=" + ecsRegion,
                        "agent.command_executor.ecs." + ecsCluster + ".subnets=" + ecsSubnets,
                        "agent.command_executor.ecs.temporal_storage.type=s3",
                        "agent.command_executor.ecs.temporal_storage.s3.bucket=" + s3Bucket,
                        "agent.command_executor.ecs.temporal_storage.s3.endpoint=" + s3Endpoint,
                        "agent.command_executor.ecs.temporal_storage.s3.credentials.access-key-id=" + accessKeyId,
                        "agent.command_executor.ecs.temporal_storage.s3.credentials.secret-access-key=" + secretAccessKey,
                        "agent.command_executor.ecs.temporal_storage.s3.direct_download=true",
                        "agent.command_executor.ecs.temporal_storage.s3.direct_download_expiration=18000",
                        "agent.command_executor.ecs.temporal_storage.s3.direct_upload=true",
                        "agent.command_executor.ecs.temporal_storage.s3.direct_upload_expiration=18000"
                )
                .build();
        server.start();

        configFile = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void testRunOnEcs()
            throws Exception
    {
        Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        Path projectDir = tempdir.resolve("py");
        Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", configFile.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/td/echo_params/echo_params.dig", projectDir.resolve("echo_params.dig"));
        copyResource("acceptance/echo_params/scripts/__init__.py", scriptsDir.resolve("__init__.py"));
        copyResource("acceptance/echo_params/scripts/echo_params.py", scriptsDir.resolve("echo_params.py"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "py",
                "-c", configFile.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        Id attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", configFile.toString(),
                    "-e", server.endpoint(),
                    "py", "echo_params",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        {
            RestSessionAttempt attempt = null;
            for (int i = 0; i < 300; i++) { // TODO heuristics should be removed
                attempt = client.getSessionAttempt(attemptId);
                if (attempt.getDone()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertThat(attempt.getSuccess(), is(true));
        }
        Thread.sleep(60 * 1000); // Log will be delayed
        String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("digdag params"));
        assertThat(logs, containsString("{'VAR_A': 'aaa'}")); // via _env in echo_params.dig
    }
}
