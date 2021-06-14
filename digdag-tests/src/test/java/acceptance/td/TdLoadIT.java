package acceptance.td;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.treasuredata.client.TDClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.UUID;

import static acceptance.td.Secrets.TD_API_KEY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.s3DeleteRecursively;
import static utils.TestUtils.s3Put;

public class TdLoadIT
{
    private static final String S3_BUCKET = System.getenv().getOrDefault("TD_LOAD_IT_S3_BUCKET", "");
    private static final String S3_ACCESS_KEY_ID = System.getenv().getOrDefault("TD_LOAD_IT_S3_ACCESS_KEY_ID", "");
    private static final String S3_SECRET_ACCESS_KEY = System.getenv().getOrDefault("TD_LOAD_IT_S3_SECRET_ACCESS_KEY", "");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path config;
    private Path projectDir;

    private TDClient client;
    private String database;

    private Path outfile;
    private AmazonS3 s3;
    private String tmpS3FolderKey;

    @Before
    public void setUp()
            throws Exception
    {
        assertThat(TD_API_KEY, not(isEmptyOrNullString()));
        assertThat(S3_BUCKET, not(isEmptyOrNullString()));
        assertThat(S3_ACCESS_KEY_ID, not(isEmptyOrNullString()));
        assertThat(S3_SECRET_ACCESS_KEY, not(isEmptyOrNullString()));

        AWSCredentials credentials = new BasicAWSCredentials(S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY);
        s3 = new AmazonS3Client(credentials);

        client = TDClient.newBuilder(false)
                .setApiKey(TD_API_KEY)
                .build();
        database = "tmp_" + UUID.randomUUID().toString().replace('-', '_');
        client.createDatabase(database);

        client.createTable(database, "td_load_test");

        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        config = folder.newFile().toPath();

        DateTimeFormatter f = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmssSSS", Locale.ROOT).withZone(UTC);
        String now = f.format(Instant.now());
        tmpS3FolderKey = "tmp/" + now + "-" + UUID.randomUUID();
        String s3DataFilePath = tmpS3FolderKey + "/data.csv";

        s3Put(s3, S3_BUCKET, s3DataFilePath, "acceptance/td/td_load/data.csv");

        Files.write(config, asList(
                "secrets.td.apikey = " + TD_API_KEY,
                "params.td.database = " + database,
                "params.s3_bucket = " + S3_BUCKET,
                "params.s3_path_prefix = " + s3DataFilePath,
                "secrets.access_key_id = " + S3_ACCESS_KEY_ID,
                "secrets.secret_access_key = " + S3_SECRET_ACCESS_KEY
        ));
        outfile = projectDir.resolve("outfile");
    }

    @After
    public void cleanUpS3()
            throws Exception
    {
        if (s3 != null && S3_BUCKET != null && tmpS3FolderKey != null) {
            s3DeleteRecursively(s3, S3_BUCKET, tmpS3FolderKey);
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
    public void testTdLoad()
            throws Exception
    {
        copyResource("acceptance/td/td_load/td_load.dig", projectDir.resolve("workflow.dig"));
        copyResource("acceptance/td/td_load/td_load_config.yml", projectDir.resolve("td_load_config.yml"));
        runWorkflow();
    }

    @Ignore("TODO: create a data connector session to use as part of the test")
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
