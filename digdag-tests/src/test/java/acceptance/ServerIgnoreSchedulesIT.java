package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSchedule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class ServerIgnoreSchedulesIT
{
    private static final String PROJECT_NAME = "foobar";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
        .addArgs("--ignore-schedules")
        .build();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve(PROJECT_NAME);
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @After
    public void shutdown()
            throws Exception
    {
        client.close();
    }

    @Test
    public void scheduleStartTime()
            throws Exception
    {
        // Create a new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/schedule/daily10.dig", projectDir.resolve("schedule.dig"));

        // Push the project starting schedules from at 2291-02-06 10:00:00 +0000
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--schedule-from", "2291-02-06 10:00:00 +0000");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        List<RestSchedule> scheds = client.getSchedules().getSchedules();
        assertThat(scheds.size(), is(0));
    }
}
