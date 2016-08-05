package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.pg.PgConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TestUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static utils.TestUtils.copyResource;

public class PgIT
{
    private static final String POSTGRESQL = System.getenv("DIGDAG_TEST_POSTGRESQL");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private String database;
    private Properties props;

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Before
    public void setUp()
    {
        if (isNullOrEmpty(POSTGRESQL)) {
            throw new IllegalStateException("Environment variable `DIGDAG_TEST_POSTGRESQL` isn't set");
        }

        props = new Properties();
        try (StringReader reader = new StringReader(POSTGRESQL)) {
            props.load(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        database = "pgoptest_" + UUID.randomUUID().toString().replace('-', '_');

        createTempDatabase(props, database);

        setupSourceTable(props, database);
    }

    @After
    public void tearDown()
    {
        removeTempDatabase(props, database);
    }

    @Test
    public void testSelectAndDownload()
            throws Exception
    {
        copyResource("acceptance/pg/select_download.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_users.sql", root().resolve("select_users.sql"));
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database=" + database, "pg.dig");

        List<String> csvLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(root().toFile(), "pg_test.csv")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                csvLines.add(line);
            }
            assertThat(csvLines.toString(), is(stringContainsInOrder(
                    Arrays.asList("id,name,score", "0,foo,3.14", "1,bar,1.23", "2,baz,5.0")
            )));
        }
    }

    private void setupSourceTable(Properties props, String database)
    {
        Config config = new ConfigFactory(new ObjectMapper()).create(
                ImmutableMap.of(
                        "host", props.get("host"),
                        "user", props.get("user"),
                        "database", database
                ));

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(config))) {
            conn.executeUpdate("CREATE TABLE users (id integer, name text, score real)");
            conn.executeUpdate("INSERT INTO users (id, name, score) VALUES (0, 'foo', 3.14)");
            conn.executeUpdate("INSERT INTO users (id, name, score) VALUES (1, 'bar', 1.23)");
            conn.executeUpdate("INSERT INTO users (id, name, score) VALUES (2, 'baz', 5.00)");
        }
    }

    private Config getOriginalDatabaseConfig(Properties props)
    {
        return new ConfigFactory(new ObjectMapper()).create(
                ImmutableMap.of(
                        "host", props.get("host"),
                        "user", props.get("user"),
                        "database", props.get("database")
                ));
    }

    private void createTempDatabase(Properties props, String tempDatabase)
    {
        Config config = getOriginalDatabaseConfig(props);
        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(config))) {
            conn.executeUpdate("CREATE DATABASE " + tempDatabase);
        }
    }

    private void removeTempDatabase(Properties props, String tempDatabase)
    {
        Config config = getOriginalDatabaseConfig(props);
        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(config))) {
            conn.executeUpdate("DROP DATABASE IF EXISTS " + tempDatabase);
        }
    }
}
