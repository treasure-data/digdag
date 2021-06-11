package acceptance;

import com.google.common.io.Files;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.database.RemoteDatabaseConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static utils.TestUtils.*;

public class CliMigrateIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
            .inProcess(false)
            .build();

    private Path config;

    private void createTestDBConfig(Path path, DatabaseConfig dbConfig)
            throws IOException
    {

        StringBuffer sbuf = new StringBuffer();
        sbuf.append(String.format("database.type = %s%n", dbConfig.getType()));
        if (dbConfig.getRemoteDatabaseConfig().isPresent()) {
            RemoteDatabaseConfig rdbConfig = dbConfig.getRemoteDatabaseConfig().get();
            sbuf.append(String.format("database.user = %s%n", rdbConfig.getUser()));
            sbuf.append(String.format("database.password = %s%n", rdbConfig.getPassword()));
            sbuf.append(String.format("database.host = %s%n", rdbConfig.getHost()));
            sbuf.append(String.format("database.database = %s%n", rdbConfig.getDatabase()));
        }
        Files.write(sbuf.toString().getBytes(UTF_8), path.toFile());
    }

    @Before
    public void setUp()
            throws Exception
    {
        config = folder.newFile().toPath();
        if ( server.isRemoteDatabase()) { // migrate command directly connect to database
            createTestDBConfig(config, server.getTestDatabaseConfig());
        }
    }

    @Test
    public void migratePostgresql()
            throws Exception
    {
        assumeTrue(server.isRemoteDatabase());

        {   // test 'digdag migrate run'
            CommandStatus status = main("migrate", "run",
                    "-c", config.toString());
            assertThat(status.code(), is(0));
            // When digdag server boot up, already all migrations should be done.
            assertThat(status.outUtf8(), containsString("No update"));
        }
        {   // test 'digdag migrate check'
            CommandStatus status = main("migrate", "check",
                    "-c", config.toString());
            assertThat(status.code(), is(0));
            // When digdag server boot up, already all migrations should be done.
            assertThat(status.outUtf8(), containsString("No update"));
        }
    }


    // This test ignore TemporaryDigdagServer and test to disk H2 database
    @Test
    public void migrateH2()
            throws Exception
    {
        assumeFalse(server.isRemoteDatabase());
        String dbPath = folder.newFolder().toString();
        {
            CommandStatus status = main("migrate", "run",
                    "-o", dbPath);
            assertThat(status.code(), is(0));
            assertThat(status.outUtf8(), containsString("successfully finished"));
        }
        {   // test 'digdag migrate check'
            CommandStatus status = main("migrate", "check",
                    "-o", dbPath);
            assertThat(status.code(), is(0));
            // all migrations should be done above 'migrate run'.
            assertThat(status.outUtf8(), containsString("No update"));
        }
    }
}
