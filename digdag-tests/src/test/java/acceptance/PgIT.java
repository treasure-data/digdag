package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.pg.PgConnectionConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TestUtils;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.base.Strings.isNullOrEmpty;
import static utils.TestUtils.copyResource;

public class PgIT
{
    private static final String POSTGRESQL = System.getenv("DIGDAG_TEST_POSTGRESQL");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void testSelectAndDownload()
            throws Exception
    {
        String database = setupPgDatabase();
        copyResource("acceptance/select_download.dig", root().resolve("pg.dig"));
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database", database, "pg.dig");
    }

    private String setupPgDatabase()
    {
        if (isNullOrEmpty(POSTGRESQL)) {
            throw new IllegalStateException("Environment variable `DIGDAG_TEST_POSTGRESQL` isn't set");
        }

        Properties props = new Properties();
        try (StringReader reader = new StringReader(POSTGRESQL)) {
            props.load(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        Config config = new ConfigFactory(new ObjectMapper()).create(
                ImmutableMap.of(
                        "host", props.get("host"),
                        "user", props.get("user"),
                        "database", props.get("database")
                ));

        String uniqueDatabase = "pgoptest_" + UUID.randomUUID().toString().replace('-', '_');

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(config))) {
            conn.executeUpdate("CREATE DATABASE " + uniqueDatabase);
            conn.executeUpdate("CONNECT TO " + uniqueDatabase);
            conn.executeUpdate("CREATE TABLE users (id integer, name text, score real)");
            conn.executeUpdate("INSERT INTO users (id, name, score) VALUES (0, 'foo', 3.14)");
            conn.executeUpdate("INSERT INTO users (id, name, score) VALUES (1, 'bar', 1.23)");
            conn.executeUpdate("INSERT INTO users (id, name, score) VALUES (2, 'baz', 5.00)");
        }

        return uniqueDatabase;
    }
}
