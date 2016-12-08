package acceptance;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.configFactory;
import static utils.TestUtils.copyResource;

public class PgIT
{
    private static final String POSTGRESQL = System.getenv("DIGDAG_TEST_POSTGRESQL");
    private static final String RESTRICTED_USER = "not_admin";
    private static final String SRC_TABLE = "src_tbl";
    private static final String DEST_TABLE = "dest_tbl";

    private static final Config EMPTY_CONFIG = configFactory().create();

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
        assumeThat(POSTGRESQL, not(isEmptyOrNullString()));

        props = new Properties();
        try (StringReader reader = new StringReader(POSTGRESQL)) {
            props.load(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        database = "pgoptest_" + UUID.randomUUID().toString().replace('-', '_');

        createTempDatabase(props, database);

        setupRestrictedUser(props, database);

        setupSourceTable(props, database);
    }

    @After
    public void tearDown()
    {
        if (props != null && database != null) {
            removeTempDatabase(props, database);
            removeRestrictedUser(props);
        }
    }

    @Test
    public void selectAndDownload()
            throws Exception
    {
        copyResource("acceptance/pg/select_download.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));
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

    @Test
    public void selectAndDownloadUsingSessionTime()
            throws Exception
    {
        copyResource("acceptance/pg/select_download.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_session_time.sql", root().resolve("select_table.sql"));
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database=" + database, "pg.dig");

        List<String> lines = Files.readAllLines(root().resolve("pg_test.csv"));
        assertThat(lines.size(), is(2));
        assertThat(lines.get(0), is("session_time"));
        assertThat(ZonedDateTime.parse(lines.get(1)), is(notNullValue()));
    }

    @Test
    public void createTableWithNestedParams()
            throws Exception
    {
        copyResource("acceptance/pg/create_table_nested.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupDestTable(props, database);

        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database=" + database, "pg.dig");

        assertTableContents(props, database, DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        ));
    }

    @Test
    public void createTableWithLocalParams()
            throws Exception
    {
        copyResource("acceptance/pg/create_table_local.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupDestTable(props, database);

        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database=" + database, "pg.dig");

        assertTableContents(props, database, DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        ));
    }

    @Test
    public void insertInto()
            throws Exception
    {
        copyResource("acceptance/pg/insert_into.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupDestTable(props, database);

        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database=" + database, "pg.dig");

        assertTableContents(props, database, DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    @Test
    public void insertIntoWithoutStrictTransaction()
            throws Exception
    {
        copyResource("acceptance/pg/insert_into_wo_st.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupDestTable(props, database);

        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "-p", "pg_database=" + database, "pg.dig");

        assertTableContents(props, database, DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    private void setupSourceTable(Properties props, String database)
    {
        SecretProvider secrets = getDatabaseSecrets(props, database);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE TABLE " + SRC_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (0, 'foo', 3.14)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (1, 'bar', 1.23)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (2, 'baz', 5.00)");

            conn.executeUpdate("GRANT SELECT ON " + SRC_TABLE +  " TO " + RESTRICTED_USER);
        }
    }

    private void setupRestrictedUser(Properties props, String database)
    {
        SecretProvider secrets = getDatabaseSecrets(props, database);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            try {
                conn.executeUpdate("CREATE ROLE " + RESTRICTED_USER);
            }
            catch (DatabaseException e) {
                // 42710: duplicate_object
                if (!e.getCause().getSQLState().equals("42710")) {
                    throw e;
                }
            }
            conn.executeUpdate("ALTER ROLE " + RESTRICTED_USER + " LOGIN");
        }
    }

    private void setupDestTable(Properties props, String database)
    {
        SecretProvider secrets = getDatabaseSecrets(props, database);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE TABLE IF NOT EXISTS " + DEST_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("DELETE FROM " + DEST_TABLE + " WHERE id = 9");
            conn.executeUpdate("INSERT INTO " + DEST_TABLE + " (id, name, score) VALUES (9, 'zzz', 9.99)");

            conn.executeUpdate("GRANT INSERT ON " + DEST_TABLE +  " TO " + RESTRICTED_USER);
        }
    }

    private void assertTableContents(Properties props, String database, String table, List<Map<String, Object>> expected)
            throws NotReadOnlyException
    {
        SecretProvider secrets = getDatabaseSecrets(props, database);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeReadOnlyQuery(String.format("SELECT * FROM %s ORDER BY id", table),
                    (rs) -> {
                        assertThat(rs.getColumnNames(), is(Arrays.asList("id", "name", "score")));
                        int index = 0;
                        List<Object> row;
                        while ((row = rs.next()) != null) {
                            Map<String, Object> expectedRow = expected.get(index);

                            int id = (int) row.get(0);
                            assertThat(id, is(expectedRow.get("id")));

                            String name = (String) row.get(1);
                            assertThat(name, is(expectedRow.get("name")));

                            float score = (float) row.get(2);
                            assertThat(score, is(expectedRow.get("score")));

                            index++;
                        }
                        assertThat(index, is(expected.size()));
                    }
            );
        }
    }

    private SecretProvider getDatabaseSecrets(Properties props, String database)
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", (String) props.get("host"),
                "user", (String) props.get("user"),
                "database", database
        ).get(key));
    }

    private SecretProvider getAdminDatabaseSecrets(Properties props)
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", (String) props.get("host"),
                "user", (String) props.get("user"),
                "database", (String) props.get("database")
        ).get(key));
    }

    private void createTempDatabase(Properties props, String tempDatabase)
    {
        SecretProvider secrets = getAdminDatabaseSecrets(props);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE DATABASE " + tempDatabase);
        }
    }

    private void removeTempDatabase(Properties props, String tempDatabase)
    {
        SecretProvider secrets = getAdminDatabaseSecrets(props);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP DATABASE IF EXISTS " + tempDatabase);
        }
    }
    private void removeRestrictedUser(Properties props)
    {
        SecretProvider secrets = getAdminDatabaseSecrets(props);

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP ROLE IF EXISTS " + RESTRICTED_USER);
        }
    }
}
