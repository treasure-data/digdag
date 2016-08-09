package acceptance.td;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandStatus;
import utils.TestUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.objectMapper;

public class TdPartialDeleteIT
{
    private static final String TD_API_KEY = System.getenv("TD_API_KEY");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;
    private String database;
    private String table;

    private Path outfile;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        Files.write(config, asList("params.td.apikey = " + TD_API_KEY));
        outfile = projectDir.resolve("outfile");

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);

        table = "test";
        String insertJobId = client.submit(TDJobRequest.newPrestoQuery(database,
                "create table " + table + " as select 1"));

        TestUtils.expect(Duration.ofMinutes(5), jobSuccess(client, insertJobId));

        String selectCountJobId = client.submit(TDJobRequest.newPrestoQuery(database, "select count(*) from " + table));
        TestUtils.expect(Duration.ofMinutes(5), jobSuccess(client, selectCountJobId));

        List<ArrayNode> result = downloadResult(selectCountJobId);
        assertThat(result.get(0).get(0).asInt(), is(1));
    }

    private List<ArrayNode> downloadResult(String jobId)
    {
        return client.jobResult(jobId, TDResultFormat.JSON, input -> {
            try {
                List<String> lines = CharStreams.readLines(new InputStreamReader(input));
                ObjectReader reader = objectMapper().readerFor(ArrayNode.class);
                List<ArrayNode> result = new ArrayList<>();
                for (String line : lines) {
                    result.add(reader.readValue(line));
                }
                return result;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @After
    public void deleteDatabase()
            throws Exception
    {
        if (client != null && database != null) {
            client.deleteDatabase(database);
        }
    }

    @Test
    public void testPartialDelete()
            throws Exception
    {
        copyResource("acceptance/td/td_partial_delete/td_partial_delete.dig", projectDir.resolve("workflow.dig"));

        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "outfile=" + outfile,
                "-p", "database=" + database,
                "-p", "table=" + table,
                "-p", "from=" + Instant.now().minus(Duration.ofDays(1)).truncatedTo(HOURS),
                "-p", "to=" + Instant.now().plus(Duration.ofDays(1)).truncatedTo(HOURS),
                "workflow.dig");

        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        assertThat(Files.exists(outfile), is(true));

        // Verify that table contents are deleted
        String selectCountJobId = client.submit(TDJobRequest.newPrestoQuery(database, "select count(*) from " + table));
        TestUtils.expect(Duration.ofMinutes(5), jobSuccess(client, selectCountJobId));
        List<ArrayNode> result = downloadResult(selectCountJobId);
        assertThat(result.get(0).get(0).asInt(), is(0));
    }

    private static Callable<Boolean> jobSuccess(TDClient client, String jobId)
    {
        return () -> {
            TDJobSummary status = client.jobStatus(jobId);
            if (status.getStatus() == TDJob.Status.SUCCESS) {
                return true;
            }
            if (status.getStatus().isFinished()) {
                fail(status.getStatus().toString());
            }
            return false;
        };
    }
}
