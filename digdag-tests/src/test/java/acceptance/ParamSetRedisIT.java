package acceptance;

import org.junit.Test;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.main;

public class ParamSetRedisIT extends BaseParamRedisIT
{
    @Test
    public void setValueToRedis()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/set.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.database.type=redis",
                "param_server.database.host=" + REDIS_HOST
        ));

        CommandStatus status = main("run",
                "-o", folder.newFolder().getAbsolutePath(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("set.dig").toString()
        );
        assertCommandStatus(status);
        assertThat("value1", is(redisClient.get("0:key1")));
        assertThat("value2", is(redisClient.get("0:key2")));
    }
}
