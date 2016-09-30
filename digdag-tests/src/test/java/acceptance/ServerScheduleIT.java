package acceptance;

import com.google.common.base.Optional;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import javax.ws.rs.NotFoundException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.pushProject;

public class ServerScheduleIT
{
    private static final String PROJECT_NAME = "foobar";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

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

        RestSchedule workflowSchedule = client.getSchedule(projectId, "schedule");
        assertThat(workflowSchedule, is(sched));

        assertThat(sched.getProject().getName(), is("foobar"));
        assertThat(sched.getNextRunTime(), is(Instant.parse("2291-02-09T00:09:00Z")));  // updated to hourly
        assertThat(sched.getNextScheduleTime(), is(OffsetDateTime.parse("2291-02-09T00:00:00Z")));
    }

    interface ScheduleModifier
    {
        void perform(DigdagClient client, String project, String workflow, int scheduleId);
    }

    @Test
    public void disableEnable()
            throws Exception
    {
        // Using client
        testDisableEnable(
                (client, project, workflow, scheduleId) ->
                {
                    client.disableSchedule(scheduleId);
                },
                (client, project, workflow, scheduleId) ->
                {
                    client.enableSchedule(scheduleId);
                });

        // Using cli and schedule id
        testDisableEnable(
                (client, project, workflow, scheduleId) ->
                {
                    CommandStatus disableStatus = main("disable",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            String.valueOf(scheduleId));
                    assertThat(disableStatus.errUtf8(), disableStatus.code(), is(0));
                    assertThat(disableStatus.outUtf8(), Matchers.containsString("Disabled schedule id: " + scheduleId));
                },
                (client, project, workflow, scheduleId) ->
                {
                    CommandStatus enableStatus = main("enable",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            String.valueOf(scheduleId));
                    assertThat(enableStatus.errUtf8(), enableStatus.code(), is(0));
                    assertThat(enableStatus.outUtf8(), Matchers.containsString("Enabled schedule id: " + scheduleId));
                });

        // Using cli and project name + workflow
        testDisableEnable(
                (client, project, workflow, scheduleId) ->
                {
                    CommandStatus disableStatus = main("disable",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            PROJECT_NAME, "schedule");
                    assertThat(disableStatus.errUtf8(), disableStatus.code(), is(0));
                    assertThat(disableStatus.outUtf8(), Matchers.containsString("Disabled schedule id: " + scheduleId));
                },
                (client, project, workflow, scheduleId) ->
                {
                    CommandStatus enableStatus = main("enable",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            PROJECT_NAME, "schedule");
                    assertThat(enableStatus.errUtf8(), enableStatus.code(), is(0));
                    assertThat(enableStatus.outUtf8(), Matchers.containsString("Enabled schedule id: " + scheduleId));
                });

        // Using cli and project name
        testDisableEnable(
                (client, project, workflow, scheduleId) ->
                {
                    CommandStatus disableStatus = main("disable",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            PROJECT_NAME);
                    assertThat(disableStatus.errUtf8(), disableStatus.code(), is(0));
                    assertThat(disableStatus.outUtf8(), Matchers.containsString("Disabled schedule id: " + scheduleId));
                },
                (client, project, workflow, scheduleId) ->
                {
                    CommandStatus enableStatus = main("enable",
                            "-c", config.toString(),
                            "-e", server.endpoint(),
                            PROJECT_NAME);
                    assertThat(enableStatus.errUtf8(), enableStatus.code(), is(0));
                    assertThat(enableStatus.outUtf8(), Matchers.containsString("Enabled schedule id: " + scheduleId));
                });
    }

    private void testDisableEnable(ScheduleModifier disable, ScheduleModifier enable)
            throws Exception
    {
        // Verify that a schedule an be disabled and enabled

        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "schedule.dig");
        pushProject(server.endpoint(), projectDir);

        List<RestSchedule> scheds = client.getSchedules();
        assertThat(scheds.size(), is(1));
        RestSchedule sched = scheds.get(0);

        disable.perform(client, PROJECT_NAME, "schedule", sched.getId());

        RestSchedule disabled = client.getSchedule(sched.getId());

        List<RestSchedule> disabledSchedules = client.getSchedules();
        assertThat(disabledSchedules.size(), is(1));
        assertThat(disabledSchedules.get(0).getId(), is(sched.getId()));
        assertThat(disabledSchedules.get(0).getDisabledAt(), is(disabled.getDisabledAt()));

        // Check that the cli lists the schedule as disabled
        {
            CommandStatus schedulesStatus = main("schedules",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(schedulesStatus.errUtf8(), schedulesStatus.code(), is(0));
            assertThat(schedulesStatus.outUtf8(), containsString("id: " + sched.getId()));
            assertThat(schedulesStatus.outUtf8(), containsString("disabled at: " + TimeUtil.formatTime(disabled.getDisabledAt().get())));
        }

        enable.perform(client, PROJECT_NAME, "schedule", sched.getId());

        RestSchedule enabled = client.getSchedule(sched.getId());

        assertThat(enabled.getId(), is(sched.getId()));
        assertThat(enabled.getDisabledAt(), is(Optional.absent()));
        List<RestSchedule> enabledSchedules = client.getSchedules();
        assertThat(enabledSchedules.size(), is(1));
        assertThat(enabledSchedules.get(0).getId(), is(sched.getId()));
        assertThat(enabledSchedules.get(0).getDisabledAt(), is(Optional.absent()));

        // Check that the cli lists the schedule as enabled
        {
            CommandStatus schedulesStatus = main("schedules",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(schedulesStatus.errUtf8(), schedulesStatus.code(), is(0));
            assertThat(schedulesStatus.outUtf8(), containsString("id: " + sched.getId()));
            assertThat(schedulesStatus.outUtf8(), containsString("disabled at: \n"));
        }
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

    @Test
    public void deleteProjectAndLookupByProjectId()
            throws Exception
    {
        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "schedule.dig");
        int projectId = pushProject(server.endpoint(), projectDir);

        // Delete project
        client.deleteProject(projectId);

        // Schedules are not available
        assertThat(client.getSchedules().size(), is(0));

        exception.expectMessage(containsString("HTTP 404 Not Found"));
        exception.expect(NotFoundException.class);
        client.getSchedules(projectId, Optional.absent());
    }

    @Test
    public void deleteProjectAndLookByName()
            throws Exception
    {
        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "schedule.dig");
        int projectId = pushProject(server.endpoint(), projectDir);

        // Delete project
        client.deleteProject(projectId);

        // Schedules are not available
        exception.expectMessage(containsString("HTTP 404 Not Found"));
        exception.expect(NotFoundException.class);
        client.getSchedule(projectId, "schedule");
    }

    @Test
    public void getScheduleByInvalidName()
            throws Exception
    {
        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "schedule.dig");
        int projectId = pushProject(server.endpoint(), projectDir);

        exception.expectMessage(containsString("schedule not found in the latest revision"));
        exception.expectMessage(not(containsString("HTTP 404 Not Found")));
        exception.expect(NotFoundException.class);
        client.getSchedule(projectId, "hieizanenryakuzi");
    }
}
