package acceptance;

import com.treasuredata.client.TDClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.main;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class TdLoadIT
{
    private static final String TD_API_KEY = System.getenv("TD_API_KEY");
    private static final String TD_LOAD_IT_SFTP_USER = System.getenv("TD_LOAD_IT_SFTP_USER");
    private static final String TD_LOAD_IT_SFTP_PASSWORD = System.getenv("TD_LOAD_IT_SFTP_PASSWORD");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;
    private String database;

    private Path outfile;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(TD_API_KEY, not(isEmptyOrNullString()));
        assumeThat(TD_LOAD_IT_SFTP_USER, not(isEmptyOrNullString()));
        assumeThat(TD_LOAD_IT_SFTP_PASSWORD, not(isEmptyOrNullString()));

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);

        client.createTable(database, "td_load_test");

        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();
        Files.write(config, asList(
                "params.td.apikey = " + TD_API_KEY,
                "params.td.database = " + database,
                "params.td_load_sftp_user = " + TD_LOAD_IT_SFTP_USER,
                "params.td_load_sftp_password = " + TD_LOAD_IT_SFTP_PASSWORD
        ));
        outfile = projectDir.resolve("outfile");
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
    public void testTdLoad()
            throws Exception
    {
        copyResource("acceptance/td/td_load/td_load.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td_load/td_load_config.yml", projectDir.resolve("td_load_config.yml"));
        runWorkflow();
    }

    @Test
    public void testTdLoadSession()
            throws Exception
    {
        copyResource("acceptance/td/td_load/td_load_session.dig", projectDir.resolve("workflow.dig"));
        runWorkflow();
    }

    private void runWorkflow()
    {
        CommandStatus runStatus = main("run",
                "-o", projectDir.toString(),
                "--config", config.toString(),
                "--project", projectDir.toString(),
                "-p", "outfile=" + outfile,
                "workflow.dig");
        assertThat(runStatus.errUtf8(), runStatus.code(), is(0));

        // TODO: verify the contents of the target table

        assertThat(Files.exists(outfile), is(true));
    }
}
