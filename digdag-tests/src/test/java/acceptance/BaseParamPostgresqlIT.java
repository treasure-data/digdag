package acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.pg.PgConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assume.assumeTrue;
import static utils.TestUtils.configFactory;

public class BaseParamPostgresqlIT
{
    protected static final String PG_PROPERTIES = System.getenv("DIGDAG_TEST_POSTGRESQL");
    protected static final String PG_IT_CONFIG = System.getenv("PG_IT_CONFIG");
    protected static final Config EMPTY_CONFIG = configFactory().create();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    protected String host;
    protected String user;
    protected String database;
    protected String tempDatabase;

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
                throw ThrowablesUtil.propagate(ex);
            }

            host = (String) props.get("host");
            user = (String) props.get("user");
            database = (String) props.get("database");
        }
        else {
            ObjectMapper objectMapper = DigdagClient.objectMapper();
            Config config = Config.deserializeFromJackson(objectMapper, objectMapper.readTree(PG_IT_CONFIG));
            host = config.get("host", String.class);
            user = config.get("user", String.class);
            database = config.get("database", String.class);
        }
        tempDatabase = "paramsetoptest_" + UUID.randomUUID().toString().replace('-', '_');

        createTempDatabase();
        createParamTable();
    }

    @After
    public void tearDown()
    {
        if (user != null) {
            if (tempDatabase != null) {
                removeTempDatabase();
            }
        }
    }

    protected SecretProvider getDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", host,
                "user", user,
                "database", tempDatabase
        ).get(key));
    }

    protected SecretProvider getAdminDatabaseSecrets()
    {
        return key -> Optional.fromNullable(ImmutableMap.of(
                "host", host,
                "user", user,
                "database", database
        ).get(key));
    }

    protected void createTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("CREATE DATABASE " + tempDatabase);
        }
    }

    protected void removeTempDatabase()
    {
        SecretProvider secrets = getAdminDatabaseSecrets();

        try (PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DROP DATABASE IF EXISTS " + tempDatabase);
        }
    }

    protected void createParamTable()
    {
        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS params (" +
                            "key text NOT NULL," +
                            "value text NOT NULL," +
                            "value_type int NOT NULL," +
                            "site_id integer," +
                            "updated_at timestamp with time zone NOT NULL," +
                            "created_at timestamp with time zone NOT NULL," +
                            "CONSTRAINT params_site_id_key_uniq UNIQUE(site_id, key)" +
                            ")"
            );
        }
    }

    protected void cleanupParamTable(){
        SecretProvider secrets = getDatabaseSecrets();
        try (
                PgConnection conn = PgConnection.open(PgConnectionConfig.configure(secrets, EMPTY_CONFIG))) {
            conn.executeUpdate("DELETE FROM params");
        }
    }
}

