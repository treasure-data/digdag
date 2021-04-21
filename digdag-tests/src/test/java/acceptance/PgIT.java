package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandStatus;
import utils.TestUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assume.assumeTrue;
import static utils.TestUtils.assertCommandStatus;
import static utils.TestUtils.configFactory;
import static utils.TestUtils.copyResource;

public class PgIT
{
    private static final Logger logger = LoggerFactory.getLogger(PgIT.class);

    private static final String PG_PROPERTIES = System.getenv("DIGDAG_TEST_POSTGRESQL");
    private static final String PG_IT_CONFIG = System.getenv("PG_IT_CONFIG");
    private static final String RESTRICTED_USER = "not_admin";
    private static final String RESTRICTED_USER_PASSWORD = "not_admin_password";
    private static final String SRC_TABLE = "src_tbl";
    private static final String DEST_TABLE = "dest_tbl";
    private static final String DATA_SCHEMA = "data_schema";
    private static final String CUSTOM_STATUS_TABLE_SCHEMA = "status_table_schema";
    private static final String CUSTOM_STATUS_TABLE = "status_table";

    private static final Config EMPTY_CONFIG = configFactory().create();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private String host;
    private String user;
    private String database;
    private String password;
    private String tempDatabase;
    private String dataSchemaName;
    private Path configFile;
    private Path configFileWithPasswordOverride;
    private Path configFileWithRestrictedUser;

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Before
    public void setUp()
            throws IOException
    {
        assumeTrue((PG_PROPERTIES != null && !PG_PROPERTIES.isEmpty())
                || (PG_IT_CONFIG != null && !PG_IT_CONFIG.isEmpty()));

        if (PG_PROPERTIES != null && !PG_PROPERTIES.isEmpty()) {
            Properties props = new Properties();
            try (StringReader reader = new StringReader(PG_PROPERTIES)) {
                props.load(reader);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

            host = (String) props.get("host");
            user = (String) props.get("user");
            password = (String) props.get("password");
            database = (String) props.get("database");
        }
        else {
            ObjectMapper objectMapper = DigdagClient.objectMapper();
            Config config = Config.deserializeFromJackson(objectMapper, objectMapper.readTree(PG_IT_CONFIG));
            host = config.get("host", String.class);
            user = config.get("user", String.class);
            password = config.get("password", String.class);
            database = config.get("database", String.class);
        }

        configFile = folder.newFile().toPath();
        Files.write(configFile, Arrays.asList("secrets.pg.password= " + password));

        configFileWithPasswordOverride = folder.newFile().toPath();
        Files.write(configFileWithPasswordOverride,
                Arrays.asList(
                        "secrets.pg.password= " + UUID.randomUUID().toString(),
                        "secrets.pg.another_password= " + password));

        configFileWithRestrictedUser = folder.newFile().toPath();
        Files.write(configFileWithRestrictedUser, Arrays.asList("secrets.pg.password= " + RESTRICTED_USER_PASSWORD));

        tempDatabase = "pgoptest_" + UUID.randomUUID().toString().replace('-', '_');

        createTempDatabase();

        setupRestrictedUser();
    }

    @After
    public void tearDown()
    {
        try {
            removeTempDatabase();
        }
        catch (Throwable e) {
            logger.error("Failed to remove resources", e);
        }

        try {
            removeRestrictedUser();
        }
        catch (Throwable e) {
            logger.error("Failed to remove resources", e);
        }
    }

    private void switchSearchPath(PgConnection conn)
    {
        if (dataSchemaName != null) {
            conn.executeUpdate(String.format("SET SEARCH_PATH TO '%s'", dataSchemaName));
        }
    }

    private void setupSchema(String schemaName)
    {
        setupSchema(schemaName, false);
    }

    private void setupSchema(String schemaName, boolean withCreatePrivilegeToRestrictedUser)
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(String.format("CREATE SCHEMA %s", schemaName));
            conn.executeUpdate(String.format("GRANT USAGE ON SCHEMA %s TO %s", schemaName, RESTRICTED_USER));
            if (withCreatePrivilegeToRestrictedUser) {
                conn.executeUpdate(String.format("GRANT CREATE ON SCHEMA %s TO %s", schemaName, RESTRICTED_USER));
            }
        }
    }

    private void prepareCustomStatusTableForRestrictedUser()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(
                    String.format(
                            "GRANT USAGE ON SCHEMA %s TO %s", CUSTOM_STATUS_TABLE_SCHEMA, RESTRICTED_USER));
            conn.executeUpdate(
                    String.format(
                            "CREATE TABLE %s.%s (query_id text NOT NULL UNIQUE, created_at timestamptz NOT NULL, completed_at timestamptz)",
                            CUSTOM_STATUS_TABLE_SCHEMA, CUSTOM_STATUS_TABLE));
            conn.executeUpdate(
                    String.format(
                            "GRANT SELECT, INSERT, UPDATE, DELETE ON %s.%s TO %s",
                            CUSTOM_STATUS_TABLE_SCHEMA, CUSTOM_STATUS_TABLE, RESTRICTED_USER));
        }
    }

    private void grantRestrictedUserOnDataSchema()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeUpdate(String.format("GRANT SELECT ON %s TO %s", SRC_TABLE, RESTRICTED_USER));
            conn.executeUpdate(String.format("GRANT INSERT ON %s TO %s", DEST_TABLE, RESTRICTED_USER));
        }
    }

    private void testSelectAndDownload(String workflowFilePath, Path configFilePath)
            throws IOException
    {
        copyResource(workflowFilePath, root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-c", configFilePath.toString(),
                "pg.dig");
        assertCommandStatus(status);

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
    public void selectAndDownload()
            throws Exception
    {
        testSelectAndDownload("acceptance/pg/select_download.dig", configFile);
    }

    @Test
    public void selectAndDownloadWithPasswordOverride()
            throws Exception
    {
        testSelectAndDownload(
                "acceptance/pg/select_download_with_password_override.dig",
                configFileWithPasswordOverride);
    }

    @Test
    public void selectAndDownloadWithNullValues()
            throws Exception
    {
        copyResource("acceptance/pg/select_download.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable(true);

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status);

        List<String> csvLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(root().toFile(), "pg_test.csv")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                csvLines.add(line);
            }
            assertThat(csvLines.toString(), is(stringContainsInOrder(
                    Arrays.asList("id,name,score", "0,,", ",bar,", ",,5.0")
            )));
        }
    }

    @Test
    public void selectAndStoreLastResults()
            throws Exception
    {
        copyResource("acceptance/pg/select_store_last_results.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-p", "outfile=out",
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status);

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(root().toFile(), "out")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            assertThat(lines.toString(), is(stringContainsInOrder(
                    Arrays.asList("foo", "bar", "baz")
            )));
        }
    }

    @Test
    public void selectAndStoreLastResultsWithFirst()
            throws Exception
    {
        copyResource("acceptance/pg/select_store_last_results_first.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-p", "outfile=out",
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status);

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(root().toFile(), "out")))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line.trim());
            }
            assertThat(lines, is(Arrays.asList("foo")));
        }
    }

    @Test
    public void selectAndStoreLastResultsWithExceedingMaxRows()
            throws Exception
    {
        copyResource("acceptance/pg/select_store_last_results.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-p", "outfile=out",
                "-X", "config.pg.max_store_last_results_rows=2",
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status, Optional.of("The number of result rows exceeded the limit"));
    }

    @Test
    public void selectAndStoreLastResultsWithExceedingMaxValueSize()
            throws Exception
    {
        copyResource("acceptance/pg/select_store_last_results.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-p", "outfile=out",
                "-X", "config.jdbc.max_store_last_results_value_size=2",
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status, Optional.of("The size of result value exceeded the limit"));
    }

    @Test
    public void createTable()
            throws Exception
    {
        copyResource("acceptance/pg/create_table.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        setupSourceTable();
        setupDestTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
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

        setupSourceTable();
        setupDestTable();

        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + user,
                "-p", "pg_database=" + tempDatabase,
                "-c", configFile.toString(),
                "pg.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    @Test
    public void insertIntoWithRestrictionOnNonPublicSchemaWithCreatePrivilege()
            throws Exception
    {
        copyResource("acceptance/pg/insert_into_with_schema.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        dataSchemaName = DATA_SCHEMA;
        setupSchema(dataSchemaName);
        setupSourceTable();
        setupDestTable();
        grantRestrictedUserOnDataSchema();

        String statusTableSchema = CUSTOM_STATUS_TABLE_SCHEMA;
        String statusTable = CUSTOM_STATUS_TABLE;
        // The user will have a privilege to create a status table
        setupSchema(statusTableSchema, true);

        CommandStatus status = TestUtils.main("run", "-o", root().toString(), "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + RESTRICTED_USER,
                "-p", "pg_database=" + tempDatabase,
                "-p", "schema_in_config=" + dataSchemaName,
                "-p", "status_table_schema_in_config=" + statusTableSchema,
                "-p", "status_table_in_config=" + statusTable,
                "-c", configFileWithRestrictedUser.toString(),
                "pg.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    @Test
    public void insertIntoWithRestrictionOnNonPublicSchemaWithoutCreatePrivilege()
            throws Exception
    {
        copyResource("acceptance/pg/insert_into_with_schema.dig", root().resolve("pg.dig"));
        copyResource("acceptance/pg/select_table.sql", root().resolve("select_table.sql"));

        dataSchemaName = DATA_SCHEMA;
        setupSchema(dataSchemaName);
        setupSourceTable();
        setupDestTable();
        grantRestrictedUserOnDataSchema();

        // The user won't have a privilege to create a status table
        // but the status table will be created on the schema in advance and the user will have proper privileges on it instead
        setupSchema(CUSTOM_STATUS_TABLE_SCHEMA, false);
        prepareCustomStatusTableForRestrictedUser();

        CommandStatus status = TestUtils.main("run", "-o", root().toString(), "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + RESTRICTED_USER,
                "-p", "pg_database=" + tempDatabase,
                "-p", "schema_in_config=" + dataSchemaName,
                "-p", "status_table_schema_in_config=" + CUSTOM_STATUS_TABLE_SCHEMA,
                "-p", "status_table_in_config=" + CUSTOM_STATUS_TABLE,
                "-c", configFileWithRestrictedUser.toString(),
                "pg.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
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

        setupSourceTable();
        setupDestTable();

        // With "strict_transaction: false", `pg` operator can work
        // even if the user can't create a new status table
        CommandStatus status = TestUtils.main(
                "run", "-o", root().toString(),
                "--project", root().toString(),
                "-p", "pg_host=" + host,
                "-p", "pg_user=" + RESTRICTED_USER,
                "-p", "pg_database=" + tempDatabase,
                "-c", configFileWithRestrictedUser.toString(),
                "pg.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    private void setupSourceTable()
    {
        setupSourceTable(false);
    }

    private void setupSourceTable(boolean withNulls)
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeUpdate("CREATE TABLE " + SRC_TABLE + " (id integer, name text, score real)");
            if (withNulls) {
                conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (0, NULL, NULL)");
                conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (NULL, 'bar', NULL)");
                conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (NULL, NULL, 5.00)");
            }
            else {
                conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (0, 'foo', 3.14)");
                conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (1, 'bar', 1.23)");
                conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (2, 'baz', 5.00)");
            }

            conn.executeUpdate("GRANT SELECT ON " + SRC_TABLE +  " TO " + RESTRICTED_USER);
        }
    }

    private void setupRestrictedUser()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            try {
                conn.executeUpdate("CREATE ROLE " + RESTRICTED_USER + " WITH PASSWORD '" + RESTRICTED_USER_PASSWORD + "'");
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

    private void setupDestTable()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeUpdate("CREATE TABLE IF NOT EXISTS " + DEST_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("DELETE FROM " + DEST_TABLE + " WHERE id = 9");
            conn.executeUpdate("INSERT INTO " + DEST_TABLE + " (id, name, score) VALUES (9, 'zzz', 9.99)");

            conn.executeUpdate("GRANT INSERT ON " + DEST_TABLE +  " TO " + RESTRICTED_USER);
        }
    }

    private void assertTableContents(String table, List<Map<String, Object>> expected)
            throws NotReadOnlyException
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
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

    private SecretProvider getDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", host,
                "user", user,
                "password", password,
                "database", tempDatabase
        ).get(key));
    }

    private SecretProvider getAdminDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", host,
                "user", user,
                "password", password,
                "database", database
        ).get(key));
    }

    private void createTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE DATABASE " + tempDatabase);
        }
    }

    private void removeTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP DATABASE IF EXISTS " + tempDatabase);
        }
    }

    private void removeRestrictedUser()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP ROLE IF EXISTS " + RESTRICTED_USER);
        }
    }
}
