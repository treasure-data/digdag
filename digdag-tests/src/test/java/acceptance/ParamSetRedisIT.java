package acceptance;

import org.junit.Test;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.main;

public class ParamSetRedisIT
        extends BaseParamRedisIT
{
    private static long TTL_90_DAYS = 60 * 60 * 24 * 90; // 90days
    private static long TTL_89_DAYS = 60 * 60 * 24 * 89; // 89days

    @Test
    public void setValueToRedis()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.type=redis",
                "param_server.host=" + DIGDAG_TEST_REDIS
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("set.dig").toString()
        );
        assertCommandStatus(status);

        long key1_ttl = redisClient.ttl("0:key1");
        long key2_ttl = redisClient.ttl("0:key2");

        assertTrue(key1_ttl <= TTL_90_DAYS && key1_ttl > TTL_89_DAYS);
        assertTrue(key2_ttl <= TTL_90_DAYS && key2_ttl > TTL_89_DAYS);

        assertThat(redisClient.get("0:key1"), is("{\"value_type\":0,\"value\":{\"value\":\"value1\"}}"));
        assertThat(redisClient.get("0:key2"), is("{\"value_type\":0,\"value\":{\"value\":\"value2\"}}"));
    }

    @Test
    public void parallelSetValueToRedis()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/parallel_set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.type=redis",
                "param_server.host=" + DIGDAG_TEST_REDIS
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("parallel_set.dig").toString()
        );
        assertCommandStatus(status);

        long key1_ttl = redisClient.ttl("0:key1");
        long key2_ttl = redisClient.ttl("0:key2");

        assertTrue(key1_ttl <= TTL_90_DAYS && key1_ttl > TTL_89_DAYS);
        assertTrue(key2_ttl <= TTL_90_DAYS && key2_ttl > TTL_89_DAYS);

        assertThat(redisClient.get("0:key1"), is("{\"value_type\":0,\"value\":{\"value\":\"value1\"}}"));
        assertThat(redisClient.get("0:key2"), is("{\"value_type\":0,\"value\":{\"value\":\"value2\"}}"));
    }
}
