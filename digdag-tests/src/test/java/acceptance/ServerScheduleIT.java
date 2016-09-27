package acceptance;

import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.pushProject;

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

        copyResource("acceptance/schedule/daily10.dig", projectDir.resolve("schedule.dig"));

        // Push the project starting schedules from at 2291-02-06 10:00:00 +0000
        {
            {
                CommandStatus pushStatus = main("push",
                        "--project", projectDir.toString(),
                        "foobar",
                        "-c", config.toString(),
                        "-e", server.endpoint(),
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

    @Test
    public void scheduleUpdatedTime()
            throws Exception
    {
        // Create a new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        int projectId;

        // Push a project that has daily schedule
        copyResource("acceptance/schedule/daily10.dig", projectDir.resolve("schedule.dig"));
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--schedule-from", "2291-02-06 10:00:00 +0000");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
            projectId = TestUtils.getProjectId(pushStatus);

        }

        // Update the project that using hourly schedule
        copyResource("acceptance/schedule/hourly9.dig", projectDir.resolve("schedule.dig"));
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--schedule-from", "2291-02-09 00:00:00 +0000");
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        List<RestSchedule> scheds = client.getSchedules();
        assertThat(scheds.size(), is(1));
        RestSchedule sched = scheds.get(0);

        List<RestSchedule> projectSchedules = client.getSchedules(projectId, Optional.absent());
        assertThat(projectSchedules, is(scheds));

        List<RestSchedule> workflowSchedules = client.getSchedules(projectId, "schedule");
        assertThat(workflowSchedules, is(scheds));

        assertThat(sched.getProject().getName(), is("foobar"));
        assertThat(sched.getNextRunTime(), is(Instant.parse("2291-02-09T00:09:00Z")));  // updated to hourly
        assertThat(sched.getNextScheduleTime(), is(OffsetDateTime.parse("2291-02-09T00:00:00Z")));
    }

    @Test
    public void disableEnable()
            throws Exception
    {
        // Verify that a schedule an be disabled and enabled

        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "schedule.dig");
        pushProject(server.endpoint(), projectDir);

        List<RestSchedule> scheds = client.getSchedules();
        assertThat(scheds.size(), is(1));
        RestSchedule sched = scheds.get(0);

        RestScheduleSummary disabled = client.disableSchedule(sched.getId());
        assertThat(disabled.getId(), is(sched.getId()));
        assertThat((double) disabled.getDisabledAt().get().getEpochSecond(), is(closeTo(Instant.now().getEpochSecond(), 30)));

        List<RestSchedule> disabledSchedules = client.getSchedules();
        assertThat(disabledSchedules.size(), is(1));
        assertThat(disabledSchedules.get(0).getId(), is(sched.getId()));
        assertThat(disabledSchedules.get(0).getDisabledAt(), is(disabled.getDisabledAt()));

        RestScheduleSummary enabled = client.enableSchedule(sched.getId());
        assertThat(enabled.getId(), is(sched.getId()));
        assertThat(enabled.getDisabledAt(), is(Optional.absent()));
        List<RestSchedule> enabledSchedules = client.getSchedules();
        assertThat(enabledSchedules.size(), is(1));
        assertThat(enabledSchedules.get(0).getId(), is(sched.getId()));
        assertThat(enabledSchedules.get(0).getDisabledAt(), is(Optional.absent()));
    }

    @Test
    public void pushDisabled()
            throws Exception
    {
        // Verify that pushing a new revision with a new schedule does not cause a it to be enabled

        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "schedule.dig");
        pushProject(server.endpoint(), projectDir);

        List<RestSchedule> schedules = client.getSchedules();
        assertThat(schedules.size(), is(1));
        RestSchedule sched = schedules.get(0);

        RestScheduleSummary disabled = client.disableSchedule(sched.getId());

        addWorkflow(projectDir, "acceptance/schedule/hourly9.dig", "schedule.dig");
        pushProject(server.endpoint(), projectDir);

        List<RestSchedule> schedulesAfterPush = client.getSchedules();
        assertThat(schedulesAfterPush.size(), is(1));
        assertThat(schedulesAfterPush.get(0).getId(), is(sched.getId()));
        assertThat(schedulesAfterPush.get(0).getDisabledAt(), is(disabled.getDisabledAt()));
    }
}
