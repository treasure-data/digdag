package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSchedule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.time.OffsetDateTime;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class RescheduleIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private DigdagClient client;
    private Path outdir;

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

        outdir = projectDir.resolve("outdir");
    }

    @Test
    public void initPushRescheduleScheduleId()
            throws Exception
    {
        // Create new project
        {
            CommandStatus cmd = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(cmd.code(), is(0));
        }

        final String projectName = "reschedule-test";
        copyResource("acceptance/reschedule/reschedule.dig", projectDir.resolve("reschedule.dig"));

        // Push
        {
            CommandStatus cmd = main("push",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectDir.toString(),
                    projectName);
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        RestSchedule oldSchedule = client.getSchedule(Id.of("1"));
        OffsetDateTime skipTo = oldSchedule.getNextScheduleTime().plusDays(1);

        // Reschedule the schedule
        {
            CommandStatus cmd = main("reschedule",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--skip-to", skipTo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")),
                    "1");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        RestSchedule newSchedule = client.getSchedule(Id.of("1"));
        assertEquals(newSchedule.getNextScheduleTime(), oldSchedule.getNextScheduleTime().plusDays(1));
    }

    @Test
    public void initPushRescheduleWorkflow()
            throws Exception
    {
        // Create new project
        {
            CommandStatus cmd = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(cmd.code(), is(0));
        }

        final String projectName = "reschedule-test";
        copyResource("acceptance/reschedule/reschedule.dig", projectDir.resolve("reschedule.dig"));

        // Push
        {
            CommandStatus cmd = main("push",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectDir.toString(),
                    projectName);
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        RestSchedule oldSchedule = client.getSchedule(Id.of("1"));
        OffsetDateTime skipTo = oldSchedule.getNextScheduleTime().plusDays(1);

        // Reschedule the schedule
        {
            CommandStatus cmd = main("reschedule",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--skip-to", skipTo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")),
                    projectName, "reschedule");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        RestSchedule newSchedule = client.getSchedule(Id.of("1"));
        assertEquals(newSchedule.getNextScheduleTime(), oldSchedule.getNextScheduleTime().plusDays(1));
    }
}
