package acceptance;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.cli.Main;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.core.Version;
import io.digdag.core.database.DataSourceProvider;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.database.RemoteDatabaseConfig;
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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalTime;
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
    private static final boolean IN_PROCESS_DEFAULT = Boolean.valueOf(System.getenv().getOrDefault("DIGDAG_TEST_TEMP_SERVER_IN_PROCESS", "false"));

    private static final String POSTGRESQL = System.getenv("DIGDAG_TEST_POSTGRESQL");

    private static final Logger log = LoggerFactory.getLogger(TemporaryDigdagServer.class);

    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true).build();

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final Optional<Version> version;

    private final String host;
    private final int port;
    private final String endpoint;
    private final List<String> extraArgs;

    private final ExecutorService executor;
    private final List<String> configuration;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private final boolean inProcess;

    private Process serverProcess;

    private Path configDirectory;
    private Path config;
    private Path taskLog;
    private Path accessLog;

    private DatabaseConfig adminDatabaseConfig;
    private DatabaseConfig testDatabaseConfig;
    private DataSource adminDataSource;

    public TemporaryDigdagServer(Builder builder)
    {
        this.version = builder.version;

        this.host = "127.0.0.1";
        // TODO (dano): Ideally the server could use system port allocation (bind on port 0) and tell
        //              us what port it got. That way we'd not have any spurious port collisions.
        this.port = findFreePort();
        this.endpoint = "http://" + host + ":" + port;
        this.configuration = new ArrayList<>(Objects.requireNonNull(builder.configuration, "configuration"));
        this.extraArgs = ImmutableList.copyOf(Objects.requireNonNull(builder.args, "args"));
        this.inProcess = builder.inProcess;

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
        for (String key : props.stringPropertyNames()) {
            config.set("database." + key, props.get(key));
        }
        config.set("database.type", "postgresql");
        config.set("database.minimumPoolSize", 0);

        // Connection configuration for administering the test database
        adminDatabaseConfig = DatabaseConfig.convertFrom(config);

        // Connection configuration for using the test database
        Config testConfig = config.deepCopy();
        String prefix = config.get("database.database", String.class, "digdag_test") + "_";
        String uniqueDatabase = prefix + UUID.randomUUID().toString().replace('-', '_');
        testConfig.set("database.database", uniqueDatabase);
        testDatabaseConfig = DatabaseConfig.convertFrom(testConfig);

        DataSourceProvider dsp = new DataSourceProvider(adminDatabaseConfig);
        adminDataSource = dsp.get();
    }

    private static class Trampoline
    {
        private static final String NAME = ManagementFactory.getRuntimeMXBean().getName();

        private static class Watchdog
                extends Thread
        {

            Watchdog()
            {
                setDaemon(false);
            }

            @Override
            public void run()
            {
                // Wait for parent to exit.
                try {
                    while (true) {
                        int c = System.in.read();
                        if (c == -1) {
                            break;
                        }
                    }
                }
                catch (IOException e) {
                    errPrefix();
                    e.printStackTrace(System.err);
                    System.err.flush();
                }
                System.err.println();
                err("child process exiting");
                // Exit with non-zero status code to skip shutdown hooks
                System.exit(-1);
            }
        }

        public static void main(String... args)
        {
            err("child process started");
            Watchdog watchdog = new Watchdog();
            watchdog.start();
            Main.main(args);
            err("child process server started");
            System.err.flush();
        }

        private static void err(String message)
        {
            errPrefix();
            System.err.println(message);
            System.err.flush();
        }

        private static void errPrefix()
        {
            System.err.print(LocalTime.now() + " [" + NAME + "] " + TemporaryDigdagServer.class.getName() + ": ");
        }
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

        if (inProcess) {
            executor.execute(() -> {
                if (version.isPresent()) {
                    main(version.get(), args, out, err);
                }
                else {
                    main(Version.buildVersion(), args, out, err);
                }
            });
        }
        else {
            String home = System.getProperty("java.home");
            String classPath = System.getProperty("java.class.path");
            Path java = Paths.get(home, "bin", "java").toAbsolutePath().normalize();

            List<String> processArgs = new ArrayList<>();
            processArgs.add(java.toString());
            processArgs.addAll(asList(
                    "-cp", classPath,
                    "-Xms128m", "-Xmx128m"));
            if (version.isPresent()) {
                processArgs.add("-D" + Version.VERSION_PROPERTY + "=" + version.get());
            }
            processArgs.add(Trampoline.class.getName());
            processArgs.addAll(args);

            ProcessBuilder processBuilder = new ProcessBuilder(processArgs);
            processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            serverProcess = processBuilder.start();
        }

        // Poll and wait for server to come up
        boolean up = false;
        DigdagClient client = DigdagClient.builder()
                .host(host)
                .port(port)
                .build();
        for (int i = 0; i < 30; i++) {
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

    private static void kill(Process p)
    {
        if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
            int pid = pid(p);
            if (pid != -1) {
                String[] cmd = {"kill", "-9", Integer.toString(pid)};
                try {
                    Runtime.getRuntime().exec(cmd);
                }
                catch (IOException e) {
                    log.warn("command failed: {}", asList(cmd), e);
                }
            }
            p.destroyForcibly();
        }
    }

    private static int pid(Process p)
    {
        if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
            try {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                return f.getInt(p);
            }
            catch (Exception ignore) {
            }
        }
        return -1;
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
        String database = remoteDatabaseConfig.get().getDatabase();
        executeQuery("UPDATE pg_database SET datallowconn = 'false' WHERE datname = '" + database + "'");
        executeQuery("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + database + "'");
        executeQuery("DROP DATABASE IF EXISTS " + database);
    }

    private void executeQuery(String query)
            throws SQLException
    {
        try (
                Connection connection = adminDataSource.getConnection();
                java.sql.Statement statement = connection.createStatement();
        ) {
            statement.execute(query);
        }
    }

    private void after()
    {
        if (serverProcess != null) {
            kill(serverProcess);
        }

        executor.shutdownNow();

        if (testDatabaseConfig != null) {
            try {
                teardownDatabase();
            }
            catch (SQLException e) {
                log.warn("Failed to tear down database", e);
            }
        }

        if (adminDataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) adminDataSource).close();
            }
            catch (Exception e) {
                log.debug("Failed to close db conn pool", e);
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
        private Builder()
        {
        }

        private List<String> args = new ArrayList<>();
        private Optional<Version> version = Optional.absent();
        private List<String> configuration = new ArrayList<>();
        private boolean inProcess = IN_PROCESS_DEFAULT;

        public Builder version(Version version)
        {
            this.version = Optional.of(version);
            return this;
        }

        public Builder configuration(String... configuration)
        {
            return configuration(asList(configuration));
        }

        public Builder configuration(Collection<String> lines)
        {
            this.configuration.addAll(lines);
            return this;
        }

        public Builder addArgs(String... args)
        {
            return addArgs(asList(args));
        }

        public Builder addArgs(Collection<String> args)
        {
            this.args.addAll(args);
            return this;
        }

        public Builder inProcess()
        {
            return inProcess(true);
        }

        public Builder inProcess(boolean inProcess)
        {
            this.inProcess = inProcess;
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
