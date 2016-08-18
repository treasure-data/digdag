package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ServerScheduleIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
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

        copyResource("acceptance/schedule/schedule.dig", projectDir.resolve("schedule.dig"));

        // Push the project starting schedules from at 2291-02-06 10:00:00 +0000
        {
            {
                CommandStatus pushStatus = main("push",
                        "--project", projectDir.toString(),
                        "foobar",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
                        "-r", "4711",
                        "--schedule-from", "2291-02-06 10:00:00 +0000");
                assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
            }

            List<RestSchedule> scheds = client.getSchedules();
            assertThat(scheds.size(), is(1));
            RestSchedule sched = scheds.get(0);

            assertThat(sched.getProject().getName(), is("foobar"));
            assertThat(sched.getNextRunTime(), is(Instant.parse("2291-02-06T10:00:00Z")));
            assertThat(sched.getNextScheduleTime(), is(OffsetDateTime.parse("2291-02-06T00:00:00Z")));
        }
    }
}
