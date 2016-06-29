package acceptance;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.core.Version;
import io.digdag.core.database.DataSourceProvider;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.database.RemoteDatabaseConfig;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.ws.rs.ProcessingException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static acceptance.TestUtils.configFactory;
import static acceptance.TestUtils.findFreePort;
import static acceptance.TestUtils.main;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TemporaryDigdagServer
        implements TestRule
{
    private static final String POSTGRESQL = System.getenv("DIGDAG_TEST_POSTGRESQL");

    private static final Logger log = LoggerFactory.getLogger(TemporaryDigdagServer.class);

    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true).build();


    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final Version version;

    private final String host;
    private final int port;
    private final String endpoint;
    private final List<String> extraArgs;

    private final ExecutorService executor;
    private final List<String> configuration;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private Path configDirectory;
    private Path config;
    private Path taskLog;
    private Path accessLog;

    private DatabaseConfig adminDatabaseConfig;
    private DatabaseConfig testDatabaseConfig;

    public TemporaryDigdagServer(Builder builder)
    {
        this.version = Objects.requireNonNull(builder.version, "version");

        this.host = "127.0.0.1";
        // TODO (dano): Ideally the server could use system port allocation (bind on port 0) and tell
        //              us what port it got. That way we'd not have any spurious port collisions.
        this.port = findFreePort();
        this.endpoint = "http://" + host + ":" + port;
        this.configuration = new ArrayList<>(Objects.requireNonNull(builder.configuration, "configuration"));
        this.extraArgs = ImmutableList.copyOf(Objects.requireNonNull(builder.args, "args"));

        this.executor = Executors.newSingleThreadExecutor(DAEMON_THREAD_FACTORY);

        if (!isNullOrEmpty(POSTGRESQL)) {
            preparePostgres();
        }
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return RuleChain
                .outerRule(temporaryFolder)
                .around(this::statement)
                .apply(base, description);
    }

    private Statement statement(Statement statement, Description description)
    {
        return new Statement()
        {
            @Override
            public void evaluate()
                    throws Throwable
            {
                before();
                try {
                    statement.evaluate();
                }
                finally {
                    after();
                }
            }
        };
    }

    private void preparePostgres()
    {
        Config config;
        Properties props = new Properties();
        try (StringReader reader = new StringReader(POSTGRESQL)) {
            props.load(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        config = TestUtils.configFactory().create();
        config.set("database.type", "postgresql");
        for (String key : props.stringPropertyNames()) {
            config.set("database." + key, props.get(key));
        }

        // Connection configuration for administering the test database
        adminDatabaseConfig = DatabaseConfig.convertFrom(config);

        // Connection configuration for using the test database
        Config testConfig = config.deepCopy();
        String prefix = config.get("database.database", String.class, "digdag_test") + "_";
        String uniqueDatabase = prefix + UUID.randomUUID().toString().replace('-', '_');
        testConfig.set("database.database", uniqueDatabase);
        testDatabaseConfig = DatabaseConfig.convertFrom(testConfig);
    }

    private void before()
            throws Throwable
    {
        setupDatabase();

        try {
            this.configDirectory = temporaryFolder.newFolder().toPath();
            this.taskLog = temporaryFolder.newFolder().toPath();
            this.accessLog = temporaryFolder.newFolder().toPath();
            this.config = Files.write(configDirectory.resolve("config"), configuration, UTF_8);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
        argsBuilder.addAll(ImmutableList.of(
                "server",
                "--port", String.valueOf(port),
                "--bind", host,
                "--access-log", accessLog.toString(),
                "-c", config.toString()));
        if (!configuration.stream().anyMatch(c -> c.contains("log-server.type"))) {
            argsBuilder.addAll(ImmutableList.of(
                    "--task-log", taskLog.toString()));
        }
        argsBuilder.addAll(extraArgs);

        List<String> args = argsBuilder.build();

        executor.execute(() -> main(version, args, out, err));

        // Poll and wait for server to come up
        boolean up = false;
        for (int i = 0; i < 30; i++) {
            DigdagClient client = DigdagClient.builder()
                    .host(host)
                    .port(port)
                    .build();
            try {
                client.getProjects();
                up = true;
                break;
            }
            catch (ProcessingException e) {
                assertThat(e.getCause(), instanceOf(ConnectException.class));
                log.debug("Waiting for server to come up...");
            }
            Thread.sleep(1000);
        }

        if (!up) {
            fail("Server failed to come up.\nout:\n" + out.toString("UTF-8") + "\nerr:\n" + err.toString("UTF-8"));
        }
    }

    private void setupDatabase()
            throws SQLException
    {
        if (testDatabaseConfig == null) {
            configuration.add("database.type = memory");
            return;
        }

        Config config = DatabaseConfig.toConfig(testDatabaseConfig, configFactory());
        for (String key : config.getKeys()) {
            configuration.add(key + " = " + config.get(key, String.class));
        }

        Optional<RemoteDatabaseConfig> remoteDatabaseConfig = testDatabaseConfig.getRemoteDatabaseConfig();
        assert remoteDatabaseConfig.isPresent();
        executeQuery("CREATE DATABASE " + remoteDatabaseConfig.get().getDatabase());
    }

    private void teardownDatabase()
            throws SQLException
    {
        Optional<RemoteDatabaseConfig> remoteDatabaseConfig = testDatabaseConfig.getRemoteDatabaseConfig();
        assert remoteDatabaseConfig.isPresent();
        executeQuery("DROP DATABASE " + remoteDatabaseConfig.get().getDatabase());
    }

    private void executeQuery(String query)
            throws SQLException
    {
        DataSourceProvider dsp = new DataSourceProvider(adminDatabaseConfig);
        DataSource ds = dsp.get();

        try (
                Connection connection = ds.getConnection();
                java.sql.Statement statement = connection.createStatement();
        ) {
            statement.execute(query);
        }
    }

    private void after()
    {
        executor.shutdownNow();

        // TODO: tear down database when it is possible to shut down the server
        if (false && testDatabaseConfig != null) {
            try {
                teardownDatabase();
            }
            catch (SQLException e) {
                log.warn("Failed to tear down database", e);
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public String endpoint()
    {
        return endpoint;
    }

    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    public String out(Charset charset)
    {
        return charset.decode(ByteBuffer.wrap(out.toByteArray())).toString();
    }

    public String err(Charset charset)
    {
        return charset.decode(ByteBuffer.wrap(err.toByteArray())).toString();
    }

    public String outUtf8()
    {
        return out(UTF_8);
    }

    public String errUtf8()
    {
        return err(UTF_8);
    }

    public static TemporaryDigdagServer of()
    {
        return builder().build();
    }

    public static TemporaryDigdagServer of(Version version)
    {
        return builder().version(version).build();
    }

    public static class Builder
    {

        private List<String> args = new ArrayList<>();

        private Builder()
        {
        }

        private Version version = Version.buildVersion();
        private List<String> configuration = new ArrayList<>();

        public Builder version(Version version)
        {
            this.version = version;
            return this;
        }

        public Builder configuration(String... configuration)
        {
            return configuration(asList(configuration));
        }

        private Builder configuration(Collection<String> lines)
        {
            this.configuration.addAll(lines);
            return this;
        }

        public Builder addArgs(String... args)
        {
            return addArgs(asList(args));
        }

        private Builder addArgs(Collection<String> args)
        {
            this.args.addAll(args);
            return this;
        }

        TemporaryDigdagServer build()
        {
            return new TemporaryDigdagServer(this);
        }
    }

    @Override
    public String toString()
    {
        return "TemporaryDigdagServer{" +
                "version=" + version +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
