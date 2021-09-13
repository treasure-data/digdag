package acceptance.td;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import io.digdag.standards.operator.redshift.RedshiftConnection;
import io.digdag.standards.operator.redshift.RedshiftConnectionConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
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
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static utils.TestUtils.*;

public class RedshiftIT
{
    private static final String REDSHIFT_CONFIG = System.getenv("REDSHIFT_IT_CONFIG");
    private static final String SRC_TABLE = "src_tbl";
    private static final String DEST_TABLE = "dest_tbl";
    private static final String DATA_SCHEMA = "data_schema";
    private static final String CUSTOM_STATUS_TABLE_SCHEMA = "status_table_schema";
    private static final String CUSTOM_STATUS_TABLE = "status_table";

    private static final Config EMPTY_CONFIG = configFactory().create();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private Path projectDir;
    private Config config;
    private String redshiftHost;
    private String redshiftDatabase;
    private String redshiftUser;
    private String redshiftPassword;
    private String s3RoleArn;
    private String s3WrongRoleArn;
    private String s3Bucket;
    private String s3ParentKey;
    private String dynamoTableName;
    private String database;
    private String restrictedUser;
    private String restrictedUserPassword;
    private String dataSchemaName;
    private Path configFile;
    private Path configFileWithPasswordOverride;
    private Path configFileWithoutFederation;
    private Path configFileWithRestrictedUser;
    private AmazonS3Client s3Client;
    private AmazonDynamoDBClient dynamoClient;

    private static class Content<T>
    {
        String sourceFileName;
        ContentBuilder<T> builder;

        Content(String sourceFileName, ContentBuilder<T> builder)
        {
            this.sourceFileName = sourceFileName;
            this.builder = builder;
        }
    }

    @FunctionalInterface
    interface ContentBuilder<T>
    {
        void build(T t) throws IOException;
    }

    @Before
    public void setUp() throws Exception
    {
        assertThat(REDSHIFT_CONFIG, not(isEmptyOrNullString()));

        ObjectMapper objectMapper = DigdagClient.objectMapper();
        config = Config.deserializeFromJackson(objectMapper, objectMapper.readTree(REDSHIFT_CONFIG));
        redshiftHost = config.get("host", String.class);
        redshiftDatabase = config.get("database", String.class);
        redshiftUser = config.get("user", String.class);
        redshiftPassword = config.get("password", String.class);
        s3Bucket = config.get("s3_bucket", String.class);
        s3RoleArn = config.get("s3_role_arn", String.class);
        s3WrongRoleArn = config.get("s3_wrong_role_arn", String.class);

        String s3AccessKeyId = config.get("s3_access_key_id", String.class);
        String s3SecretAccessKey = config.get("s3_secret_access_key", String.class);
        AWSCredentials credentials = new BasicAWSCredentials(s3AccessKeyId, s3SecretAccessKey);
        s3Client = new AmazonS3Client(credentials);
        s3ParentKey = UUID.randomUUID().toString();

        dynamoTableName = UUID.randomUUID().toString();
        dynamoClient = new AmazonDynamoDBClient(credentials);

        database = "redshiftoptest_" + UUID.randomUUID().toString().replace('-', '_');
        restrictedUser = "user_" + UUID.randomUUID().toString().replace('-', '_');
        restrictedUserPassword = UUID.randomUUID() + "0aZ";

        projectDir = folder.getRoot().toPath().toAbsolutePath().normalize();
        configFile = folder.newFile().toPath();
        Files.write(configFile, asList(
                "secrets.aws.redshift.password= " + redshiftPassword,
                "secrets.aws.redshift_unload.access_key_id=" + s3AccessKeyId,
                "secrets.aws.redshift_load.access_key_id=" + s3AccessKeyId,
                "secrets.aws.secret_access_key=" + s3SecretAccessKey
        ));

        configFileWithPasswordOverride = folder.newFile().toPath();
        Files.write(configFileWithPasswordOverride, asList(
                "secrets.aws.redshift.password= " + UUID.randomUUID().toString(),
                "secrets.aws.redshift.another_password= " + redshiftPassword,
                "secrets.aws.redshift_unload.access_key_id=" + s3AccessKeyId,
                "secrets.aws.redshift_load.access_key_id=" + s3AccessKeyId,
                "secrets.aws.secret_access_key=" + s3SecretAccessKey
        ));

        String s3AccessKeyIdWithoutFederation = config.get("s3_access_key_id_wo_federation", String.class);
        String s3SecretAccessKeyWithoutFederation = config.get("s3_secret_access_key_wo_federation", String.class);
        configFileWithoutFederation = folder.newFile().toPath();
        Files.write(configFileWithoutFederation, asList(
                "secrets.aws.redshift.password= " + redshiftPassword,
                "secrets.aws.access_key_id=" + s3AccessKeyIdWithoutFederation,
                "secrets.aws.access_key_id=" + s3AccessKeyIdWithoutFederation,
                "secrets.aws.redshift.secret_access_key=" + s3SecretAccessKeyWithoutFederation
        ));

        configFileWithRestrictedUser = folder.newFile().toPath();
        Files.write(configFileWithRestrictedUser, asList(
                "secrets.aws.redshift.password= " + restrictedUserPassword,
                "secrets.aws.redshift_unload.access_key_id=" + s3AccessKeyId,
                "secrets.aws.redshift_load.access_key_id=" + s3AccessKeyId,
                "secrets.aws.secret_access_key=" + s3SecretAccessKey
        ));

        createTempDatabase();

        setupRestrictedUser();
    }

    @After
    public void tearDown()
    {
        if (redshiftHost == null) {
            // Do nothing since all the tests in this class are skipped
            return;
        }

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

    private void testSelectAndDownload(String workflowFilePath, Path configFilePath)
            throws IOException
    {
        copyResource(workflowFilePath, projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupSourceTable();

        Path resultFile = folder.newFolder().toPath().resolve("result.csv");
        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "download_file_in_config=" + resultFile.toString(),
                "-c", configFilePath.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        assertThat(Files.exists(resultFile), is(true));

        List<String> csvLines = Files.readAllLines(resultFile);
        assertThat(csvLines.toString(), is(stringContainsInOrder(
                asList("id,name,score", "0,foo,3.14", "1,bar,1.23", "2,baz,5.0")
        )));
    }

    @Test
    public void selectAndDownload()
            throws Exception
    {
        testSelectAndDownload("acceptance/redshift/select_download.dig", configFile);
    }

    @Test
    public void selectAndDownloadWithPasswordOverride()
            throws Exception
    {
        testSelectAndDownload(
                "acceptance/redshift/select_download_with_password_override.dig",
                configFileWithPasswordOverride);
    }

    @Test
    public void selectAndStoreResult()
            throws Exception
    {
        copyResource("acceptance/redshift/select_store_last_results.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "outfile=out",
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(projectDir.toFile(), "out")), UTF_8))) {
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
    public void selectAndStoreResultsWithFirst()
            throws Exception
    {
        copyResource("acceptance/redshift/select_store_last_results_first.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupSourceTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "outfile=out",
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(projectDir.toFile(), "out")), UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line.trim());
            }
            assertThat(lines, is(Arrays.asList("foo")));
        }
    }

    @Test
    public void createTable()
            throws Exception
    {
        copyResource("acceptance/redshift/create_table.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupSourceTable();
        setupDestTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        ));

        List<String> statusTables = listStatusTables();
        assertThat(statusTables.size(), is(1));

        String statusTable = statusTables.get(0);
        assertThat(statusTable, notNullValue());
    }

    @Test
    public void createTableWithShortTTLStatusTable()
            throws Exception
    {
        copyResource("acceptance/redshift/create_table_with_short_ttl_status_table.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        setupSourceTable();
        setupDestTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        List<String> statusTables = listStatusTables();
        assertThat(statusTables.size(), is(0));
    }

    private List<String> listStatusTables()
            throws NotReadOnlyException
    {
        SecretProvider secrets = getDatabaseSecrets();

        List<String> statusTables = new ArrayList<>();
        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeReadOnlyQuery(
                    "SELECT tablename FROM pg_tables WHERE tablename LIKE '__digdag_status_%%'",
                    rs -> {
                        List<Object> row = null;
                        while ((row = rs.next()) != null) {
                            assertThat(row, notNullValue());
                            assertThat(row.size(), is(1));
                            statusTables.add((String) row.get(0));
                        }
                    }
            );
        }
        return statusTables;
    }

    @Test
    public void insertIntoWithRestrictionOnNonPublicSchemaWithCreatePrivilege()
            throws Exception
    {
        copyResource("acceptance/redshift/insert_into_with_schema.dig", projectDir.resolve("redshift.dig"));
        copyResource("acceptance/redshift/select_table.sql", projectDir.resolve("select_table.sql"));

        dataSchemaName = DATA_SCHEMA;
        setupSchema(dataSchemaName);
        setupSourceTable();
        setupDestTable();
        grantRestrictedUserOnTheSchema();

        String statusTableSchema = CUSTOM_STATUS_TABLE_SCHEMA;
        String statusTable = CUSTOM_STATUS_TABLE;
        // The user will have a privilege to create a status table
        setupSchema(statusTableSchema, true);

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + restrictedUser,
                "-p", "schema_in_config=" + dataSchemaName,
                "-p", "status_table_schema_in_config=" + statusTableSchema,
                "-p", "status_table_in_config=" + statusTable,
                "-c", configFileWithRestrictedUser.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        assertTableContents(DEST_TABLE, Arrays.asList(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        ));
    }

    // Redshift doesn't support row-level locks, so we don't execute a test
    // similar to acceptance.PgIT.insertIntoWithRestrictionOnNonPublicSchemaWithoutCreatePrivilege.

    private void setupSchema(String schemaName)
    {
        setupSchema(schemaName, false);
    }

    private void setupSchema(String schemaName, boolean withCreatePrivilegeToRestrictedUser)
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(String.format("CREATE SCHEMA %s", schemaName));
            conn.executeUpdate(String.format("GRANT USAGE ON SCHEMA %s TO %s", schemaName, restrictedUser));
            if (withCreatePrivilegeToRestrictedUser) {
                conn.executeUpdate(String.format("GRANT CREATE ON SCHEMA %s TO %s", schemaName, restrictedUser));
            }
        }
    }

    private void grantRestrictedUserOnTheSchema()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeUpdate(String.format("GRANT SELECT ON %s TO %s", SRC_TABLE, restrictedUser));
            conn.executeUpdate(String.format("GRANT INSERT ON %s TO %s", DEST_TABLE, restrictedUser));
        }
    }

    private void switchSearchPath(RedshiftConnection conn)
    {
        if (dataSchemaName != null) {
            conn.executeUpdate(String.format("SET SEARCH_PATH TO '%s'", dataSchemaName));
        }
    }

    private void setupSourceTable()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeUpdate("CREATE TABLE " + SRC_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (0, 'foo', 3.14)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (1, 'bar', 1.23)");
            conn.executeUpdate("INSERT INTO " + SRC_TABLE + " (id, name, score) VALUES (2, 'baz', 5.00)");

            conn.executeUpdate("GRANT SELECT ON " + SRC_TABLE +  " TO " + restrictedUser);
        }
    }

    private void setupRestrictedUser()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            try {
                conn.executeUpdate("CREATE USER " + restrictedUser + " WITH PASSWORD '" + restrictedUserPassword + "'");
            }
            catch (DatabaseException e) {
                // 42710: duplicate_object
                if (!e.getCause().getSQLState().equals("42710")) {
                    throw e;
                }
            }
        }
    }

    private void setupDestTable()
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeUpdate("CREATE TABLE IF NOT EXISTS " + DEST_TABLE + " (id integer, name text, score real)");
            conn.executeUpdate("DELETE FROM " + DEST_TABLE + " WHERE id = 9");
            conn.executeUpdate("INSERT INTO " + DEST_TABLE + " (id, name, score) VALUES (9, 'zzz', 9.99)");

            conn.executeUpdate("GRANT INSERT ON " + DEST_TABLE +  " TO " + restrictedUser);
        }
    }

    private void assertTableContents(String table, List<Map<String, Object>> expected)
            throws NotReadOnlyException
    {
        SecretProvider secrets = getDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            switchSearchPath(conn);
            conn.executeReadOnlyQuery(String.format("SELECT * FROM %s ORDER BY id", table),
                    (rs) -> {
                        assertThat(rs.getColumnNames(), is(asList("id", "name", "score")));
                        int index = 0;
                        List<Object> row;
                        while ((row = rs.next()) != null) {
                            assertThat(index, lessThan(expected.size()));

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
                "host", redshiftHost,
                "user", redshiftUser,
                "password", redshiftPassword,
                "database", database
        ).get(key));
    }

    private SecretProvider getAdminDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", redshiftHost,
                "user", redshiftUser,
                "password", redshiftPassword,
                "database", redshiftDatabase
        ).get(key));
    }

    private void createTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE DATABASE " + database);
        }
    }

    private void removeTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            // Redshift doesn't support 'DROP DATABASE IF EXISTS'...
            conn.executeUpdate("DROP DATABASE " + database);
        }
    }

    private void removeRestrictedUser()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (RedshiftConnection conn = RedshiftConnection.open(RedshiftConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP USER IF EXISTS " + restrictedUser);
        }
    }

    private void buildContentAsBufferedWriter(File file, ContentBuilder<BufferedWriter> builder)
            throws IOException
    {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF_8))) {
            builder.build(writer);
        }
    }

    private void buildContentAsOutputStream(File file, ContentBuilder<OutputStream> builder)
            throws IOException
    {
        try (OutputStream output = new FileOutputStream(file)) {
            builder.build(output);
        }
    }

    @Test
    public void loadCSVFileFromS3()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0,foo,3.14");
                                    w.newLine();
                                    w.write("1,bar,1.23");
                                    w.newLine();
                                    w.write("2,baz,5.0");
                                    w.newLine();
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_csv.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );

        List<String> statusTables = listStatusTables();
        assertThat(statusTables.size(), is(1));

        String statusTable = statusTables.get(0);
        assertThat(statusTable, notNullValue());
    }

    @Test
    public void loadCSVFileFromS3WithRole()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0,foo,3.14");
                                    w.newLine();
                                    w.write("1,bar,1.23");
                                    w.newLine();
                                    w.write("2,baz,5.0");
                                    w.newLine();
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_csv_with_role.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadCSVFileFromS3WithWrongRole()
            throws Exception
    {
        copyResource("acceptance/redshift/load_from_s3_csv_with_role.dig", projectDir.resolve("redshift.dig"));
        setupSourceTable();
        setupDestTable();
        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "table_in_config=" + DEST_TABLE,
                "-p", "from_in_config=" + String.format("s3://%s/%s", s3Bucket, s3ParentKey),
                "-p", "role_arn_in_config=" + s3WrongRoleArn,
                "-c", configFile.toString(),
                "redshift.dig");
        // TODO: Check an error message
        assertThat(status.code(), is(1));
    }

    @Test
    public void loadCSVFileFromS3WithoutFederation()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0,foo,3.14");
                                    w.newLine();
                                    w.write("1,bar,1.23");
                                    w.newLine();
                                    w.write("2,baz,5.0");
                                    w.newLine();
                                })
                        )
                ),
                configFileWithoutFederation,
                "acceptance/redshift/load_from_s3_csv_without_temp_credentials.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadCSVFileFromS3WithShortTTLStatusTable()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0,foo,3.14");
                                    w.newLine();
                                    w.write("1,bar,1.23");
                                    w.newLine();
                                    w.write("2,baz,5.0");
                                    w.newLine();
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_csv_with_short_ttl_status_table.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );

        List<String> statusTables = listStatusTables();
        assertThat(statusTables.size(), is(0));
    }

    @Test
    public void loadCSVFileFromS3WithManifest()
            throws Exception
    {
        String manifest = String.format( "{\n" +
                "  \"entries\": [\n" +
                "    {\"url\":\"s3://%s/%s/testdata0.data\",\"mandatory\":true},\n" +
                "    {\"url\":\"s3://%s/%s/testdata1.data\",\"mandatory\":true},\n" +
                "    {\"url\":\"s3://%s/%s/testdata3.data\",\"mandatory\":false}\n" +
                "   ]\n" +
                "}",
                s3Bucket, s3ParentKey,
                s3Bucket, s3ParentKey,
                s3Bucket, s3ParentKey
        );

        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0,foo,3.14");
                                    w.newLine();
                                    w.write("1,bar,1.23");
                                    w.newLine();
                                    w.write("2,baz,5.0");
                                    w.newLine();
                                })
                        ),
                        new Content<>(
                                "testdata1.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("3,bow-wow-wow,1.25");
                                    w.newLine();
                                    w.write("4,meow-meow,4.5");
                                    w.newLine();
                                })
                        ),
                        new Content<>(
                                "testdata2.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("5,don't include me!,0");
                                    w.newLine();
                                })
                        ),
                        new Content<>(
                                "my-manifest",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write(manifest);
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_csv_with_manifest.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 3, "name", "bow-wow-wow", "score", 1.25f),
                        ImmutableMap.of("id", 4, "name", "meow-meow", "score", 4.5f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadCSVFileFromS3WithNoload()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0,foo,3.14");
                                    w.newLine();
                                    w.write("1,bar,1.23");
                                    w.newLine();
                                    w.write("2,baz,5.0");
                                    w.newLine();
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_csv_with_noload.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadCSVFileFromS3WithManyOptions()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0$`foo`$3.14");
                                    w.newLine();
                                    w.write("1$bar$`1.23`");
                                    w.newLine();
                                    w.write("`2`$baz$5.0");
                                    w.newLine();
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_csv_with_many_options.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadFixedWidthFileFromS3WithRestrictionOnPublicSchema()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("0foo3.14");
                                    w.newLine();
                                    w.write("1bar1.23");
                                    w.newLine();
                                    w.write("2baz5.00");
                                    w.newLine();
                                })
                        )
                ),
                configFileWithRestrictedUser,
                "acceptance/redshift/load_from_s3_fixedwidth_with_schema.dig",
                restrictedUser,
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                ),
                Optional.of(() -> {
                    dataSchemaName = DATA_SCHEMA;
                    setupSchema(dataSchemaName);
                    setupSourceTable();
                    setupDestTable();
                    grantRestrictedUserOnTheSchema();

                    String statusTableSchema = CUSTOM_STATUS_TABLE_SCHEMA;
                    setupSchema(statusTableSchema, true);
                })
        );
    }

    @Test
    public void loadJsonFileFromS3()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("{\"id\":0,\"name\":\"foo\",\"score\":3.14}{\"id\":1,\"name\":\"bar\",\"score\":1.23}");
                                    w.newLine();
                                    w.write("{\"id\":2,\"name\":\"baz\",\"score\":5.0}");
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_json.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadJsonFileFromS3WithJsonPathFile()
            throws Exception
    {
        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("{\"xid\":0,\"xname\":\"foo\",\"xscore\":3.14}");
                                    w.write("{\"xid\":1,\"xname\":\"bar\",\"xscore\":1.23}");
                                    w.write("{\"xid\":2,\"xname\":\"baz\",\"xscore\":5.0}");
                                })
                        ),
                        new Content<>(
                                "my-json-path-file",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("{\"jsonpaths\": [\"$['xid']\", \"$['xname']\", \"$['xscore']\"]}");
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_json_with_json_path_file.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    private byte[] avroTestData(List<Schema.Field> fields, List<Map<String, Object>> records)
            throws IOException
    {
        Schema schema = Schema.createRecord("testdata", null, null, false);
        schema.setFields(fields);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(datum);
        writer.create(schema, out);
        for (Map<String, Object> record : records) {
            GenericData.Record r = new GenericData.Record(schema);
            for (Map.Entry<String, Object> item : record.entrySet()) {
                r.put(item.getKey(), item.getValue());
            }
            writer.append(r);
        }
        writer.close();

        return out.toByteArray();
    }

    @Test
    public void loadAvroFileFromS3()
            throws Exception

   {
        byte[] avroTestData = avroTestData(
                ImmutableList.of(
                        new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
                        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
                        new Schema.Field("score", Schema.create(Schema.Type.FLOAT), null, null)
                ),
                ImmutableList.of(
                        ImmutableMap.of(
                                "id", 0,
                                "name", "foo",
                                "score", 3.14f),
                        ImmutableMap.of(
                                "id", 1,
                                "name", "bar",
                                "score", 1.23f),
                        ImmutableMap.of(
                                "id", 2,
                                "name", "baz",
                                "score", 5.0f)
                ));

        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsOutputStream(f, o -> {
                                    o.write(avroTestData);
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_avro.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadAvroFileFromS3JsonPathFile()
            throws Exception
    {
        byte[] avroTestData = avroTestData(
                ImmutableList.of(
                        new Schema.Field("xid", Schema.create(Schema.Type.INT), null, null),
                        new Schema.Field("xname", Schema.create(Schema.Type.STRING), null, null),
                        new Schema.Field("xscore", Schema.create(Schema.Type.FLOAT), null, null)
                ),
                ImmutableList.of(
                        ImmutableMap.of(
                                "xid", 0,
                                "xname", "foo",
                                "xscore", 3.14f),
                        ImmutableMap.of(
                                "xid", 1,
                                "xname", "bar",
                                "xscore", 1.23f),
                        ImmutableMap.of(
                                "xid", 2,
                                "xname", "baz",
                                "xscore", 5.0f)
                )
        );

        loadFromS3AndAssert(
                Arrays.asList(
                        new Content<>(
                                "testdata0.data",
                                f -> buildContentAsOutputStream(f, o -> {
                                    o.write(avroTestData);
                                })
                        ),
                        new Content<>(
                                "my-json-path-file",
                                f -> buildContentAsBufferedWriter(f, w -> {
                                    w.write("{\"jsonpaths\": [\"$['xid']\", \"$['xname']\", \"$['xscore']\"]}");
                                })
                        )
                ),
                configFile,
                "acceptance/redshift/load_from_s3_avro_with_json_path_file.dig",
                Arrays.asList(
                        ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                        ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                        ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                        ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
                )
        );
    }

    @Test
    public void loadFromDynamoDB()
            throws Exception
    {
        DynamoDB dynamoDB = new DynamoDB(dynamoClient);

        ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(dynamoTableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(1L)
                        .withWriteCapacityUnits(1L));

        ImmutableList<Item> items = ImmutableList.of(
                new Item().withPrimaryKey("Id", 0).withString("Name", "foo").withNumber("Score", 3.14f),
                new Item().withPrimaryKey("Id", 1).withString("Name", "bar").withNumber("Score", 1.23f),
                new Item().withPrimaryKey("Id", 2).withString("Name", "baz").withNumber("Score", 5.0f)
        );

        ImmutableList<Map<String, Object>> expected = ImmutableList.of(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f),
                ImmutableMap.of("id", 9, "name", "zzz", "score", 9.99f)
        );

        Table table = null;
        try {
            table = dynamoDB.createTable(request);
            table.waitForActive();

            items.forEach(table::putItem);

            runDigdagWithDynamoDB(configFile, "acceptance/redshift/load_from_dynamodb.dig", redshiftUser, Optional.absent());

            assertTableContents(DEST_TABLE, expected);
        }
        finally {
            if (table != null) {
                table.delete();
                table.waitForDelete();
            }
        }
    }

    @Test
    public void unloadToS3()
            throws Exception
    {
        copyResource("acceptance/redshift/unload_to_s3.dig", projectDir.resolve("redshift.dig"));

        setupSourceTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "to_in_config=" + String.format("s3://%s/%s", s3Bucket, s3ParentKey),
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        ImmutableList<Map<String, Object>> expected = ImmutableList.of(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        );

        assertS3Contents(expected);
    }

    @Test
    public void unloadToS3WithoutFederation()
            throws Exception
    {
        Files.move(configFileWithoutFederation, configFile, StandardCopyOption.REPLACE_EXISTING);

        copyResource("acceptance/redshift/unload_to_s3_wo_temp_credentials.dig", projectDir.resolve("redshift.dig"));

        setupSourceTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "to_in_config=" + String.format("s3://%s/%s", s3Bucket, s3ParentKey),
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        ImmutableList<Map<String, Object>> expected = ImmutableList.of(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        );

        assertS3Contents(expected);
    }

    @Test
    public void unloadToS3WithManifest()
            throws Exception
    {
        copyResource("acceptance/redshift/unload_to_s3_with_manifest.dig", projectDir.resolve("redshift.dig"));

        setupSourceTable();

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUser,
                "-p", "to_in_config=" + String.format("s3://%s/%s", s3Bucket, s3ParentKey),
                "-c", configFile.toString(),
                "redshift.dig");
        assertCommandStatus(status);

        ImmutableList<Map<String, Object>> expected = ImmutableList.of(
                ImmutableMap.of("id", 0, "name", "foo", "score", 3.14f),
                ImmutableMap.of("id", 1, "name", "bar", "score", 1.23f),
                ImmutableMap.of("id", 2, "name", "baz", "score", 5.0f)
        );

        assertS3Contents(expected);
    }

    private void assertS3Contents(List<Map<String, Object>> expected)
            throws IOException
    {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(s3ParentKey);
        ListObjectsV2Result result;
        List<String> lines = new ArrayList<>();
        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                if (objectSummary.getKey().endsWith("_part_00")) {
                    try (BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            s3Client.getObject(
                                                    objectSummary.getBucketName(),
                                                    objectSummary.getKey()).getObjectContent(), UTF_8))) {
                        lines.addAll(reader.lines().collect(Collectors.toList()));
                    }
                }
                else {
                    assertThat(objectSummary.getKey(), endsWith("_manifest"));
                }

                try {
                    s3Client.deleteObject(objectSummary.getBucketName(), objectSummary.getKey());
                }
                catch (Exception e) {
                    logger.warn("Failed to delete S3 object: bucket={}, key={}", s3Bucket, objectSummary.getKey(), e);
                }
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());

        List<ImmutableMap<String, ? extends Serializable>> actual =
                lines.stream()
                        .map(
                                l -> {
                                    String[] values = l.split("\\|");
                                    assertThat(values.length, is(3));
                                    return ImmutableMap.of(
                                            "id", Integer.valueOf(values[0]),
                                            "name", values[1],
                                            "score", Float.valueOf(values[2]));
                                }
                        )
                        .sorted((o1, o2) -> ((Integer)o1.get("id")) - ((Integer) o2.get("id")))
                        .collect(Collectors.toList());
        assertThat(actual, is(expected));
    }

    private void runDigdagWithS3(Path configFilePath, String resourceFileName, String redshiftUserName, Optional<Runnable> prepare)
            throws IOException
    {
        runDigdag(configFilePath, resourceFileName, redshiftUserName, String.format("s3://%s/%s", s3Bucket, s3ParentKey), prepare);
    }

    private void runDigdagWithDynamoDB(Path configFilePath, String resourceFileName, String redshiftUserName, Optional<Runnable> prepare)
            throws IOException
    {
        runDigdag(configFilePath, resourceFileName, redshiftUserName, String.format("dynamodb://%s", dynamoTableName), prepare);
    }

    private void runDigdag(Path configFilePath, String resourceFileName, String redshiftUserName, String fromUri, Optional<Runnable> prepare)
            throws IOException
    {
        copyResource(resourceFileName, projectDir.resolve("redshift.dig"));

        if (prepare.isPresent()) {
            prepare.get().run();
        }
        else {
            setupSourceTable();
            setupDestTable();
        }

        CommandStatus status = TestUtils.main("run", "-o", projectDir.toString(), "--project", projectDir.toString(),
                "-p", "redshift_database=" + database,
                "-p", "redshift_host=" + redshiftHost,
                "-p", "redshift_user=" + redshiftUserName,
                "-p", "table_in_config=" + DEST_TABLE,
                "-p", "from_in_config=" + fromUri,
                // Most diddag configs don't use the followings
                "-p", "role_arn_in_config=" + s3RoleArn,
                "-p", "schema_in_config=" + DATA_SCHEMA,
                "-p", "status_table_schema_in_config=" + CUSTOM_STATUS_TABLE_SCHEMA,
                "-p", "status_table_in_config=" + CUSTOM_STATUS_TABLE,
                "-c", configFilePath.toString(),
                "redshift.dig");
        assertCommandStatus(status);
    }

    private void loadFromS3AndAssert(
            List<Content<File>> contents,
            Path configFilePath,
            String resourceFileName,
            List<Map<String, Object>> expected)
            throws Exception
    {
        loadFromS3AndAssert(contents, configFilePath, resourceFileName, redshiftUser, expected, Optional.absent());
    }

    private void loadFromS3AndAssert(
            List<Content<File>> contents,
            Path configFilePath,
            String resourceFileName,
            String redshiftUserName,
            List<Map<String, Object>> expected,
            Optional<Runnable> prepare)
            throws Exception
    {
        s3Client.createBucket(s3Bucket);
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        temporaryFolder.getRoot().deleteOnExit();
        List<String> keys = new ArrayList<>();
        try {
            for (Content<File> content : contents) {
                File tmpFile = temporaryFolder.newFile();
                tmpFile.deleteOnExit();
                String key = s3ParentKey + "/" + content.sourceFileName;
                content.builder.build(tmpFile);

                s3Client.putObject(s3Bucket, key, tmpFile);
                keys.add(key);
            }

            runDigdagWithS3(configFilePath, resourceFileName, redshiftUserName, prepare);

            assertTableContents(DEST_TABLE, expected);
        }
        finally {
            for (String key : keys) {
                try {
                    s3Client.deleteObject(s3Bucket, key);
                }
                catch (Exception e) {
                    logger.warn("Failed to delete S3 object: bucket={}, key={}", s3Bucket, key, e);
                }
            }
        }
    }
}
