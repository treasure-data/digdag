package acceptance.td;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static acceptance.td.Secrets.TD_API_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.main;

public class TdRunIT
{
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
        assertThat(TD_API_KEY, not(isEmptyOrNullString()));
        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        Files.write(config, ImmutableList.of("secrets.td.apikey = " + TD_API_KEY));
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
        String name = "test_" + UUID.randomUUID().toString().replace('-', '_');
        createQuery(name);
        testRunSavedQuery(name);
    }

    @Test
    public void testRunSavedQueryWithUnicodeName()
            throws Exception
    {
        String name = "test query with  space\tand 漢字　and räksmörgås " + UUID.randomUUID().toString().replace('-', '_');
        createQuery(name);
        testRunSavedQuery(name);
    }

    @Test
    public void testRunSavedQueryById()
            throws Exception
    {
        String name = "test_" + UUID.randomUUID().toString().replace('-', '_');
        TDSavedQuery query = createQuery(name);
        testRunSavedQuery(query.getId());
    }

    private TDSavedQuery createQuery(String name)
    {
        return saveQuery(TDSavedQuery.newBuilder(
                name,
                TDJob.Type.PRESTO,
                database,
                "select 1",
                "Asia/Tokyo")
                .build());
    }

    private void testRunSavedQuery(String queryReference)
            throws IOException
    {
        Files.write(projectDir.resolve("workflow.dig"),
                Resources.toString(Resources.getResource("acceptance/td/td_run/td_run.dig"), UTF_8)
                        .replace("${saved_query_reference}", queryReference).getBytes(UTF_8));

        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
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
