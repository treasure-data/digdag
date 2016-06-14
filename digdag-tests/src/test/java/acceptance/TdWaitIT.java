package acceptance;

import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static acceptance.TestUtils.addWorkflow;
import static acceptance.TestUtils.attemptFailure;
import static acceptance.TestUtils.attemptSuccess;
import static acceptance.TestUtils.createProject;
import static acceptance.TestUtils.expect;
import static acceptance.TestUtils.pushAndStart;
import static acceptance.TestUtils.runWorkflow;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class TdWaitIT
{
    private static final String TD_API_KEY = System.getenv("TD_API_KEY");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "params.td.apikey = " + TD_API_KEY,
                    "config.td.wait.min_poll_interval = 5s")
            .build();

    private Path projectDir;

    private String tempDatabase;

    private TDClient tdClient;
    private Path outfile;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, is(notNullValue()));
        projectDir = folder.getRoot().toPath();
        createProject(projectDir);

        tdClient = TDClient.newBuilder(false).setApiKey(TD_API_KEY).build();
        tempDatabase = "tmp_" + UUID.randomUUID().toString().replace("-", "_");

        tdClient.createDatabase(tempDatabase);

        outfile = folder.getRoot().toPath().resolve("outfile").toAbsolutePath().normalize();
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (tdClient != null && tempDatabase != null) {
            tdClient.deleteDatabase(tempDatabase);
        }
    }

    @Test
    public void testTdWaitForTableThatAlreadyExists()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/td/wait/td_wait.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                .put("wait_poll_interval", "5s")
                .put("wait_table", "nasdaq")
                .put("wait_limit", "1")
                .put("wait_rows", "1")
                .put("database", "sample_datasets")
                .put("outfile", outfile.toString())
                .build());
        expect(Duration.ofSeconds(30), attemptSuccess(server.endpoint(), attemptId));
    }

    @Test
    public void testTdWaitForTableThatDoesNotYetExist()
            throws Exception
    {
        String table = "td_wait_test";

        addWorkflow(projectDir, "acceptance/td/wait/td_wait.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                .put("td.apikey", TD_API_KEY)
                .put("database", tempDatabase)
                .put("wait_poll_interval", "5s")
                .put("wait_table", table)
                .put("wait_limit", "2")
                .put("wait_rows", "2")
                .put("outfile", outfile.toString())
                .build());

        // Verify that the workflow does not proceed beyond running
        {
            Thread.sleep(10_000);
            CommandStatus attemptsStatus = TestUtils.main("attempts",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: running"));
        }

        // Create the table (empty)
        runWorkflow("acceptance/td/wait/create_table.dig", ImmutableMap.of(
                "td.apikey", TD_API_KEY,
                "database", tempDatabase,
                "table", table));

        // Verify that the workflow still does not proceed beyond running
        {
            Thread.sleep(10_000);
            CommandStatus attemptsStatus = TestUtils.main("attempts",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: running"));
        }

        // Insert a row
        runWorkflow("acceptance/td/wait/insert_into.dig", ImmutableMap.of(
                "td.apikey", TD_API_KEY,
                "database", tempDatabase,
                "table", table,
                "query", "select 1"));

        // Verify that the workflow still does not proceed beyond running
        {
            Thread.sleep(10_000);
            CommandStatus attemptsStatus = TestUtils.main("attempts",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: running"));
        }

        // Verify that the output file does not yet exist
        assertThat(Files.exists(outfile), is(false));

        // Insert another row to trigger the workflow to complete
        runWorkflow("acceptance/td/wait/insert_into.dig", ImmutableMap.of(
                "td.apikey", TD_API_KEY,
                "database", tempDatabase,
                "table", table,
                "query", "select 1"));

        // Verify that the workflow completes
        expect(Duration.ofSeconds(30), attemptSuccess(server.endpoint(), attemptId));

        // Check that the task after the td_wait executed and the output file exists
        assertThat(Files.exists(outfile), is(true));
    }

    @Test
    public void verifyThatTooSmallPollIntervalFails()
            throws Exception
    {
        addWorkflow(projectDir, "acceptance/td/wait/td_wait.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                .put("wait_poll_interval", "1s")
                .put("wait_table", "nasdaq")
                .put("wait_limit", "1")
                .put("wait_rows", "1")
                .put("database", "sample_datasets")
                .put("outfile", outfile.toString())
                .build());
        expect(Duration.ofSeconds(30), attemptFailure(server.endpoint(), attemptId));
    }
}
