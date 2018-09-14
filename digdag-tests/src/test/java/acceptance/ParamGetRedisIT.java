package acceptance;

import org.junit.Test;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.main;

public class ParamGetRedisIT
        extends BaseParamRedisIT
{
    @Test
    public void testGetParams()
            throws IOException
    {
        Path projectDir = folder.newFolder().toPath();
        addWorkflow(projectDir, "acceptance/params/get.dig");
        Path config = projectDir.resolve("config");
        Files.write(config, asList(
                "param_server.database.type=redis",
                "param_server.database.host=" + REDIS_HOST
        ));
        String output = folder.newFolder().getAbsolutePath();

        redisClient.set("0:key1", "{\"value\":{\"value\":\"value1\"}, \"value_type\":0}");
        redisClient.set("0:key2", "{\"value\":{\"value\":\"value2\"}, \"value_type\":0}");

        CommandStatus status = main("run",
                "-o", output,
                "--config", config.toString(),
                "--project", projectDir.toString(),
                projectDir.resolve("get.dig").toString()
        );

        assertCommandStatus(status);
        assertThat(new String(Files.readAllBytes(projectDir.resolve("out"))).trim(), is("value1 value2"));
    }
}
