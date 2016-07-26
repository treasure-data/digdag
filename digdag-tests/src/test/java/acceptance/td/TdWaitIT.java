package acceptance.td;

import utils.CommandStatus;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClient;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.custom.combined.CombinedParameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptFailure;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.attempts;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static utils.TestUtils.main;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.runWorkflow;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitParamsRunner.class)
public class TdWaitIT
{
    private static final String TD_API_KEY = System.getenv("TD_API_KEY");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .configuration(
                    "params.td.apikey = " + TD_API_KEY,
                    "config.td.wait.min_poll_interval = 5s"
            )
            .build();

    protected Path projectDir;

    protected String tempDatabase;

    protected TDClient tdClient;
    protected Path outfile;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
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

    public static class BadQueryFailureIT
            extends TdWaitIT
    {
        @Test
        public void test()
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait_bad_query.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_bad_query", ImmutableMap.<String, String>builder()
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(Duration.ofSeconds(300), attemptFailure(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(false));
        }
    }

    public static class TableThatAlreadyExistsWithDefaults
            extends TdWaitIT
    {
        @Test
        public void test()
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait_defaults.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_defaults", ImmutableMap.<String, String>builder()
                    .put("wait_table", "nasdaq")
                    .put("wait_rows", "1")
                    .put("database", "sample_datasets")
                    .build());
            expect(Duration.ofSeconds(300), attemptSuccess(server.endpoint(), attemptId));
        }
    }

    public static class QuirkyQueries
            extends TdWaitIT
    {
        @Test
        @Parameters({"td_wait_query_with_comments.dig", "td_wait_query_with_semicolon.dig"})
        public void test(String workflow)
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/" + workflow, "workflow.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "workflow", ImmutableMap.<String, String>builder()
                    .put("wait_table", "nasdaq")
                    .put("wait_rows", "1")
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(Duration.ofSeconds(300), attemptSuccess(server.endpoint(), attemptId));
        }
    }

    public static class TableThatAlreadyExists
            extends TdWaitIT
    {

        @Test
        @Parameters({"hive", "presto"})
        public void test(String engine)
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                    .put("wait_poll_interval", "5s")
                    .put("wait_table", "nasdaq")
                    .put("wait_limit", "1")
                    .put("table_wait_rows", "1")
                    .put("query_wait_rows", "1")
                    .put("wait_engine", engine)
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(Duration.ofSeconds(300), attemptSuccess(server.endpoint(), attemptId));
        }
    }

    public static class Truthiness
            extends TdWaitIT
    {

        @Test
        @CombinedParameters({"hive, presto"})
        public void test(String engine)
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait_truthiness.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_truthiness", ImmutableMap.<String, String>builder()
                    .put("wait_engine", engine)
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(Duration.ofSeconds(300), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));
        }
    }

    public static class Falsiness
            extends TdWaitIT
    {
        @Test
        @CombinedParameters({"hive, presto",
                             "0, false, NULL"})
        public void test(String engine, String selectValue)
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait_falsiness.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_falsiness", ImmutableMap.<String, String>builder()
                    .put("select_value", selectValue)
                    .put("wait_engine", engine)
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            Thread.sleep(engine.equals("hive") ? 60_000 : 10_000);
            CommandStatus status = attempts(server.endpoint(), attemptId);
            assertThat(status.outUtf8(), containsString("status: running"));
            assertThat(Files.exists(outfile), is(false));
        }
    }

    public static class TableThatDoesNotYetExistHiveIT
            extends TdWaitIT
    {

        @Test
        public void test()
                throws Exception
        {
            super.testTdWaitForTableThatDoesNotYetExist(false, 60, "hive");
        }
    }

    public static class TableWithRowsThatDoesNotYetExistHiveIT
            extends TdWaitIT
    {

        @Test
        public void test()
                throws Exception
        {
            super.testTdWaitForTableThatDoesNotYetExist(true, 60, "hive");
        }
    }

    public static class TableThatDoesNotYetExistPrestoIT
            extends TdWaitIT
    {

        @Test
        public void test()
                throws Exception
        {
            super.testTdWaitForTableThatDoesNotYetExist(false, 10, "presto");
        }
    }

    public static class TableWithRowsThatDoesNotYetExistPrestoIT
            extends TdWaitIT
    {

        @Test
        public void test()
                throws Exception
        {
            super.testTdWaitForTableThatDoesNotYetExist(true, 10, "presto");
        }
    }

    protected void testTdWaitForTableThatDoesNotYetExist(boolean tableWaitRows, int sleep, String engine)
            throws Exception
    {
        String table = "td_wait_test";

        addWorkflow(projectDir, "acceptance/td/td_wait/td_wait.dig");
        long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                .put("td.apikey", TD_API_KEY)
                .put("database", tempDatabase)
                .put("wait_poll_interval", "5s")
                .put("wait_table", table)
                .put("wait_limit", "2")
                .put("table_wait_rows", tableWaitRows ? "2" : "0")
                .put("query_wait_rows", "2")
                .put("wait_engine", engine)
                .put("outfile", outfile.toString())
                .build());

        // Verify that the workflow does not proceed beyond running
        {
            Thread.sleep(sleep * 1000);
            CommandStatus attemptsStatus = TestUtils.main("attempts",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: running"));
        }

        // Create the table (empty)
        runWorkflow("acceptance/td/td_wait/create_table.dig", ImmutableMap.of(
                "td.apikey", TD_API_KEY,
                "database", tempDatabase,
                "table", table));

        // Verify that the workflow still does not proceed beyond running
        {
            Thread.sleep(sleep * 1000);
            CommandStatus attemptsStatus = TestUtils.main("attempts",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: running"));
        }

        // Insert a row
        runWorkflow("acceptance/td/td_wait/insert_into.dig", ImmutableMap.of(
                "td.apikey", TD_API_KEY,
                "database", tempDatabase,
                "table", table,
                "query", "select 1"));

        // Verify that the workflow still does not proceed beyond running
        {
            Thread.sleep(sleep * 1000);
            CommandStatus attemptsStatus = TestUtils.main("attempts",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(attemptsStatus.outUtf8(), containsString("status: running"));
        }

        // Verify that the output file does not yet exist
        assertThat(Files.exists(outfile), is(false));

        // Insert another row to trigger the workflow to complete
        runWorkflow("acceptance/td/td_wait/insert_into.dig", ImmutableMap.of(
                "td.apikey", TD_API_KEY,
                "database", tempDatabase,
                "table", table,
                "query", "select 1"));

        // Verify that the workflow completes
        expect(Duration.ofSeconds(300), attemptSuccess(server.endpoint(), attemptId));

        // Check that the task after the td_wait executed and the output file exists
        assertThat(Files.exists(outfile), is(true));
    }

    public static class TooSmallPollIntervalFailureIT
            extends TdWaitIT
    {
        @Test
        public void test()
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait.dig");
            long attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                    .put("wait_engine", "presto")
                    .put("wait_poll_interval", "1s")
                    .put("wait_table", "nasdaq")
                    .put("wait_limit", "1")
                    .put("table_wait_rows", "1")
                    .put("query_wait_rows", "1")
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(Duration.ofSeconds(300), attemptFailure(server.endpoint(), attemptId));
            CommandStatus logStatus = main("log",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    Long.toString(attemptId));
            assertThat(logStatus.errUtf8(), logStatus.code(), is(0));
            assertThat(logStatus.outUtf8(), containsString("poll interval must be at least"));
        }
    }
}
