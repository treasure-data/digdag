package acceptance;

import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSchedule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.expect;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static java.time.temporal.ChronoUnit.HOURS;

public class AttemptLimitIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server;

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    private void setUpServerWithSystemProps()
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .inProcess(false)  // setting system property doesn't work with in-process mode
                .systemProperty("io.digdag.limits.maxAttempts", "3")
                .build();
        server.start();
        setupClient();
    }

    private void setUpServerWithConfig()
            throws Exception
    {
        server = TemporaryDigdagServer.builder()
                .inProcess(false)  // setting system property doesn't work with in-process mode
                .systemProperty("io.digdag.limits.maxAttempts", "10") //system property must be overridden by config
                .configuration("executor.attempt_max_run = 3")
                .build();
        server.start();
        setupClient();
    }

    private void setupClient()
    {
        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void startFailsWithTooManyAttempts()
            throws Exception
    {
        setUpServerWithSystemProps();
        startFailsWithTooManyAttempts0();
    }

    @Test
    public void startFailsWithTooManyAttemptsWithConfig()
            throws Exception
    {
        setUpServerWithConfig();
        startFailsWithTooManyAttempts0();
    }

    private void startFailsWithTooManyAttempts0()
            throws Exception
    {

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));

        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // Start 3 sessions
        for (int i = 0; i < 3; i++) {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "hourly_sleep",
                    "--session", "2016-01-0" + (i + 1));
            assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        }

        // Next attempt fails with 400 Bad Request
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "foobar", "hourly_sleep",
                    "--session", "2016-01-04");
            assertThat(startStatus.errUtf8(), startStatus.code(), is(1));
            assertThat(startStatus.errUtf8(), containsString("Too many attempts running"));
            assertThat(startStatus.errUtf8(), containsString("\"status\":400"));
        }
    }

    @Test
    public void scheduleWithAttemptLimit()
            throws Exception
    {
        setUpServerWithSystemProps();
        scheduleWithAttemptLimit0();
    }

    @Test
    public void scheduleWithAttemptLimitWithConfig()
            throws Exception
    {
        setUpServerWithConfig();
        scheduleWithAttemptLimit0();
    }

    private void scheduleWithAttemptLimit0()
            throws Exception
    {

        // Create a new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        // Push hourly schedule from now minus 10 hours
        Instant startTime = Instant.now().minus(Duration.ofHours(10)).truncatedTo(HOURS);

        copyResource("acceptance/attempt_limit/hourly_sleep.dig", projectDir.resolve("hourly_sleep.dig"));
        {
            CommandStatus pushStatus = main("push",
                    "--project", projectDir.toString(),
                    "foobar",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--schedule-from", Long.toString(startTime.getEpochSecond()));
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // Wait until next run time becomes later than now
        expect(Duration.ofMinutes(5), () -> {
            List<RestSchedule> scheds = client.getSchedules().getSchedules();
            return scheds.size() > 0 && scheds.get(0).getNextRunTime().isAfter(Instant.now());
        });

        // Number of actually submitted sessions should be 3 = maxAttempts
        assertThat(client.getSessionAttempts(Optional.absent(), Optional.absent()).getAttempts().size(), is(3));

        // Although next run time > now, next schedule time is 3-attempt later than start time
        assertThat(client.getSchedules().getSchedules().get(0).getNextScheduleTime().toInstant(), is(startTime.plus(Duration.ofHours(3))));
    }
}
