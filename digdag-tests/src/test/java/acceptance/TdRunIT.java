package acceptance;

import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class TdRunIT
{
    private static final String TD_API_KEY = System.getenv("TD_API_KEY");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;
    private String database;

    private final List<String> savedQueries = new ArrayList<>();
    private Path outfile;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        Files.write(config, ("params.td.apikey = " + TD_API_KEY).getBytes("UTF-8"));
        outfile = projectDir.resolve("outfile");

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);
    }

    @After
    public void deleteQueries()
            throws Exception
    {
        if (client != null) {
            for (String savedQuery : savedQueries) {
                client.deleteSavedQuery(savedQuery);
            }
        }
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
    public void testRunSavedQuery()
            throws Exception
    {
        testRunSavedQuery("test_" + UUID.randomUUID().toString().replace('-', '_'));
    }

    @Test
    public void testRunSavedQueryWithUnicodeName()
            throws Exception
    {
        testRunSavedQuery("test query with  space\tand 漢字　and räksmörgås " + UUID.randomUUID().toString().replace('-', '_'));
    }

    private void testRunSavedQuery(String queryName)
            throws IOException
    {
        TDSavedQuery savedQuery = saveQuery(TDSavedQuery.newBuilder(
                queryName,
                TDJob.Type.PRESTO,
                database,
                "select 1",
                "Asia/Tokyo")
                .build());

        copyResource("acceptance/td/td_run/td_run.dig", projectDir.resolve("workflow.dig"));

        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "saved_query_name=" + savedQuery.getName(),
                "-p", "outfile=" + outfile,
                "workflow.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        assertThat(Files.exists(outfile), is(true));
    }

    private TDSavedQuery saveQuery(TDSaveQueryRequest request)
    {
        savedQueries.add(request.getName());
        return client.saveQuery(request);
    }
}
