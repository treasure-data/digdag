package acceptance.td;

import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.UUID;

import static acceptance.td.Secrets.TD_API_ENDPOINT;
import static acceptance.td.Secrets.TD_API_KEY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.main;

public class TdJobShowIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private Path projectDir;

    private TDClient client;
    private String database;
    private String table;
    private String savedQuery;
    private String jobId;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        assumeThat(TD_API_ENDPOINT, not(isEmptyOrNullString()));

        projectDir = folder.newFolder().toPath().toAbsolutePath().normalize();

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .setEndpoint(TD_API_ENDPOINT)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);
        String queryName = "test_" + UUID.randomUUID().toString().replace('-', '_');
        createQuery(queryName);
        TDSavedQueryStartRequest req = TDSavedQueryStartRequest.builder()
                .name(queryName)
                .scheduledTime(new Date())
                .build();
        this.jobId = client.startSavedQuery(req);
        this.table = "test_" + UUID.randomUUID().toString().replace('-', '_');
        client.createTableIfNotExists(database, table);
    }

    @Test
    public void testShowJobWithoutOption()
            throws IOException
    {
        String output = folder.newFolder().getAbsolutePath();
        addWorkflow(projectDir, "acceptance/td/td_job_show/without_option.dig");

        CommandStatus status = main("run",
                "-o", output,
                "--project", projectDir.toString(),
                "-p", "database=" + database,
                "-p", "queryName=" + savedQuery,
                projectDir.resolve("without_option.dig").toString()
        );
        assertCommandStatus(status);
        assertThat(new String(Files.readAllBytes(projectDir.resolve("out"))).trim(), is("1 SUCCESS"));
    }

    @Test
    public void testShowJobWithOption()
            throws IOException
    {
        String output = folder.newFolder().getAbsolutePath();
        addWorkflow(projectDir, "acceptance/td/td_job_show/with_option.dig");

        CommandStatus status = main("run",
                "-o", output,
                "--project", projectDir.toString(),
                "-p", "database=" + database,
                "-p", "queryName=" + savedQuery,
                projectDir.resolve("with_option.dig").toString()
        );
        assertCommandStatus(status);
        assertThat(new String(Files.readAllBytes(projectDir.resolve("out"))).trim(), is("1 SUCCESS"));
    }

    @After
    public void deleteQuery()
            throws Exception
    {
        if (client != null) {
            client.deleteSavedQuery(savedQuery);
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

    private TDSavedQuery saveQuery(TDSaveQueryRequest request)
    {
        this.savedQuery = request.getName();
        return client.saveQuery(request);
    }
}
