package acceptance.td;

import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClient;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.custom.combined.CombinedParameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.littleshoot.proxy.HttpProxyServer;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static acceptance.td.Secrets.TD_API_KEY;
import static acceptance.td.Secrets.secretsServerConfiguration;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptFailure;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.attempts;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static utils.TestUtils.main;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.pushProject;
import static utils.TestUtils.runWorkflow;

@RunWith(JUnitParamsRunner.class)
public class TdWaitIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public TemporaryDigdagServer server;

    protected Path projectDir;
    protected String projectName;
    protected Id projectId;

    protected String tempDatabase;

    protected TDClient tdClient;
    protected Path outfile;

    protected DigdagClient digdagClient;

    private HttpProxyServer proxyServer;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(TD_API_KEY, not(isEmptyOrNullString()));

        proxyServer = TestUtils.startRequestFailingProxy(5);

        server = TemporaryDigdagServer.builder()
                .configuration(secretsServerConfiguration())
                .configuration("config.td.wait.min_poll_interval = 5s")
                .configuration(
                        "params.td.use_ssl = true",
                        "params.td.proxy.enabled = true",
                        "params.td.proxy.host = " + proxyServer.getListenAddress().getHostString(),
                        "params.td.proxy.port = " + proxyServer.getListenAddress().getPort()
                )
                .build();
        server.start();

        projectDir = folder.getRoot().toPath();
        createProject(projectDir);
        projectName = projectDir.getFileName().toString();
        projectId = pushProject(server.endpoint(), projectDir, projectName);

        tdClient = TDClient.newBuilder(false).setApiKey(TD_API_KEY).build();
        tempDatabase = "tmp_" + UUID.randomUUID().toString().replace("-", "_");

        tdClient.createDatabase(tempDatabase);

        outfile = folder.getRoot().toPath().resolve("outfile").toAbsolutePath().normalize();

        digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "td.apikey", TD_API_KEY);
    }

    @After
    public void tearDownDigdagServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @After
    public void tearDownProxyServer()
            throws Exception
    {
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (tdClient != null && tempDatabase != null) {
            tdClient.deleteDatabase(tempDatabase);
        }
    }

    private static Duration expectDuration()
    {
        return expectDuration("presto");
    }

    private static Duration expectDuration(String kind)
    {
        switch (kind) {
            case "hive":
                return Duration.ofMinutes(15);
            default:
                return Duration.ofMinutes(5);
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
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_bad_query", ImmutableMap.<String, String>builder()
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(expectDuration(), attemptFailure(server.endpoint(), attemptId));
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
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_defaults", ImmutableMap.<String, String>builder()
                    .put("wait_table", "nasdaq")
                    .put("wait_rows", "1")
                    .put("database", "sample_datasets")
                    .build());
            expect(expectDuration(), attemptSuccess(server.endpoint(), attemptId));
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
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "workflow", ImmutableMap.<String, String>builder()
                    .put("wait_table", "nasdaq")
                    .put("wait_rows", "1")
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(expectDuration(), attemptSuccess(server.endpoint(), attemptId));
        }
    }

    @Ignore
    public static class TableThatAlreadyExists
            extends TdWaitIT
    {

        @Test
        @Parameters({"hive", "presto"})
        public void test(String engine)
                throws Exception
        {
            addWorkflow(projectDir, "acceptance/td/td_wait/td_wait.dig");
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                    .put("wait_poll_interval", "5s")
                    .put("wait_table", "nasdaq")
                    .put("wait_limit", "1")
                    .put("table_wait_rows", "1")
                    .put("query_wait_rows", "1")
                    .put("wait_engine", engine)
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(expectDuration(engine), attemptSuccess(server.endpoint(), attemptId));
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
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_truthiness", ImmutableMap.<String, String>builder()
                    .put("wait_engine", engine)
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(expectDuration(engine), attemptSuccess(server.endpoint(), attemptId));
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
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait_falsiness", ImmutableMap.<String, String>builder()
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
        Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
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
        runWorkflow(folder, "acceptance/td/td_wait/create_table.dig", ImmutableMap.of(
                "database", tempDatabase,
                "table", table),
                ImmutableMap.of("secrets.td.apikey", TD_API_KEY));

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
        runWorkflow(folder, "acceptance/td/td_wait/insert_into.dig", ImmutableMap.of(
                "database", tempDatabase,
                "table", table,
                "query", "select 1"),
                ImmutableMap.of("secrets.td.apikey", TD_API_KEY));

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
        runWorkflow(folder, "acceptance/td/td_wait/insert_into.dig", ImmutableMap.of(
                "database", tempDatabase,
                "table", table,
                "query", "select 1"),
                ImmutableMap.of("secrets.td.apikey", TD_API_KEY));

        // Verify that the workflow completes
        expect(expectDuration(engine), attemptSuccess(server.endpoint(), attemptId));

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
            Id attemptId = pushAndStart(server.endpoint(), projectDir, "td_wait", ImmutableMap.<String, String>builder()
                    .put("wait_engine", "presto")
                    .put("wait_poll_interval", "1s")
                    .put("wait_table", "nasdaq")
                    .put("wait_limit", "1")
                    .put("table_wait_rows", "1")
                    .put("query_wait_rows", "1")
                    .put("database", "sample_datasets")
                    .put("outfile", outfile.toString())
                    .build());
            expect(expectDuration(), attemptFailure(server.endpoint(), attemptId));
            CommandStatus logStatus = main("log",
                    "-c", "/dev/null",
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            assertThat(logStatus.errUtf8(), logStatus.code(), is(0));
            assertThat(logStatus.outUtf8(), containsString("poll interval must be at least"));
        }
    }
}
