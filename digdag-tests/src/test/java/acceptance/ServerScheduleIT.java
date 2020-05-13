package acceptance;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSessionAttempt;
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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
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

            List<RestSchedule> scheds = client.getSchedules().getSchedules();
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

        Id projectId;

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

        List<RestSchedule> scheds = client.getSchedules().getSchedules();
        assertThat(scheds.size(), is(1));
        RestSchedule sched = scheds.get(0);

        List<RestSchedule> projectSchedules = client.getSchedules(projectId, Optional.absent()).getSchedules();
        assertThat(projectSchedules, is(scheds));

        RestSchedule workflowSchedule = client.getSchedule(projectId, "schedule");
        assertThat(workflowSchedule, is(sched));

        assertThat(sched.getProject().getName(), is("foobar"));
        assertThat(sched.getNextRunTime(), is(Instant.parse("2291-02-09T00:09:00Z")));  // updated to hourly
        assertThat(sched.getNextScheduleTime(), is(OffsetDateTime.parse("2291-02-09T00:00:00Z")));
    }

    interface ScheduleModifier
    {
        void perform(DigdagClient client, String project, String workflow, Id scheduleId);
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

        List<RestSchedule> scheds = client.getSchedules().getSchedules();
        assertThat(scheds.size(), is(1));
        RestSchedule sched = scheds.get(0);

        disable.perform(client, PROJECT_NAME, "schedule", sched.getId());

        RestSchedule disabled = client.getSchedule(sched.getId());

        List<RestSchedule> disabledSchedules = client.getSchedules().getSchedules();
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
        List<RestSchedule> enabledSchedules = client.getSchedules().getSchedules();
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

        List<RestSchedule> schedules = client.getSchedules().getSchedules();
        assertThat(schedules.size(), is(1));
        RestSchedule sched = schedules.get(0);

        RestScheduleSummary disabled = client.disableSchedule(sched.getId());

        addWorkflow(projectDir, "acceptance/schedule/hourly9.dig", "schedule.dig");
        pushProject(server.endpoint(), projectDir);

        List<RestSchedule> schedulesAfterPush = client.getSchedules().getSchedules();
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
        Id projectId = pushProject(server.endpoint(), projectDir);

        // Delete project
        client.deleteProject(projectId);

        // Schedules are not available
        assertThat(client.getSchedules().getSchedules().size(), is(0));

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
        Id projectId = pushProject(server.endpoint(), projectDir);

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
        Id projectId = pushProject(server.endpoint(), projectDir);

        exception.expectMessage(containsString("schedule not found in the latest revision"));
        exception.expectMessage(not(containsString("HTTP 404 Not Found")));
        exception.expect(NotFoundException.class);
        client.getSchedule(projectId, "hieizanenryakuzi");
    }

    @Test
    public void testSkipOnOvertime()
            throws Exception
    {
        File outfile = folder.newFile();
        Files.createDirectories(projectDir);

        Files.write(projectDir.resolve("schedule.dig"), asList(Resources.toString(
                Resources.getResource("acceptance/schedule/skip_on_overtime.dig"), UTF_8)
                .replace("${outfile}", outfile.toString())));
        pushProject(server.endpoint(), projectDir, "schedule");

        TestUtils.expect(Duration.ofMinutes(1),
                () -> Files.readAllLines(outfile.toPath()).size() >= 6);

        List<String> lines = Files.readAllLines(outfile.toPath()).stream()
                .limit(8)
                .collect(Collectors.toList());

        String unixtime1 = lines.get(0).split(": ")[1];
        String lastUnixtime1 = lines.get(1).split(": ")[1];

        // These lines will be empty as there are no preceding processed / skipped sessions
        assertThat(lines.get(2).trim(), is("last_executed_session_unixtime:"));

        String unixtime2 = lines.get(3).split(": ")[1];
        String lastUnixtime2 = lines.get(4).split(": ")[1];
        String lastProcessedUnixTime2 = lines.get(5).split(": ")[1];

        long lastUnixtime1Epoch = Long.parseLong(lastUnixtime1);
        long lastUnixtime2Epoch = Long.parseLong(lastUnixtime2);
        long lastProcessedUnixTime2Epoch = Long.parseLong(lastProcessedUnixTime2);
        long unixtime1Epoch = Long.parseLong(unixtime1);
        long unixtime2Epoch = Long.parseLong(unixtime2);

        assertThat(lastProcessedUnixTime2, is(unixtime1));
        assertThat(lastUnixtime1Epoch, is(unixtime1Epoch - 2));
        assertThat(lastUnixtime2Epoch, is(unixtime2Epoch - 2));
        assertThat(unixtime2Epoch, Matchers.greaterThanOrEqualTo(unixtime1Epoch + 10));

        List<RestSessionAttempt> attempts = client.getSessionAttempts(Optional.absent(), Optional.absent()).getAttempts();

        RestSessionAttempt attempt1 = attempts.stream()
                .filter(a -> a.getSessionTime().toEpochSecond() == unixtime1Epoch)
                .findAny().get();

        RestSessionAttempt attempt2 = attempts.stream()
                .filter(a -> a.getSessionTime().toEpochSecond() == unixtime2Epoch)
                .findAny().get();

        assertThat(attempt1.getParams().has("last_executed_session_time"), is(false));

        assertThat(Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(attempt2.getParams()
                        .get("last_executed_session_time", String.class))),
                is(Instant.ofEpochSecond(lastProcessedUnixTime2Epoch)));
    }

    @Test
    public void testSkipAndEnable()
            throws Exception
    {
        Files.createDirectories(projectDir);
        addWorkflow(projectDir, "acceptance/schedule/daily10.dig", "daily10.dig");
        addWorkflow(projectDir, "acceptance/schedule/hourly9.dig", "hourly9.dig");
        pushProject(server.endpoint(), projectDir);

        List<RestSchedule> schedules = client.getSchedules().getSchedules();

        RestSchedule daily = schedules.get(0);
        RestSchedule hourly = schedules.get(1);
        Optional<String> skipToTime = Optional.of("2291-02-09T00:01:00Z");

        // daily
        // schedule is 10:00:00 every day
        {
            client.disableSchedule(daily.getId());
            client.enableSchedule(daily.getId(), true, skipToTime);
            // get schedule
            RestSchedule enabled = client.getSchedule(daily.getId());
            assertThat(enabled.getDisabledAt(), is(Optional.absent()));
            assertThat(enabled.getNextRunTime(), is(Instant.parse("2291-02-09T10:00:00Z")));
            assertThat(enabled.getNextScheduleTime(), is(OffsetDateTime.parse("2291-02-09T00:00Z")));
        }

        // hourly
        // schedule is 09:00 every hour
        {
            client.disableSchedule(hourly.getId());
            client.enableSchedule(hourly.getId(), true, skipToTime);
            // get schedule
            RestSchedule enabled = client.getSchedule(hourly.getId());
            assertThat(enabled.getDisabledAt(), is(Optional.absent()));
            assertThat(enabled.getNextRunTime(), is(Instant.parse("2291-02-09T00:09:00Z")));
            assertThat(enabled.getNextScheduleTime(), is(OffsetDateTime.parse("2291-02-09T00:00Z")));
        }
    }
}
