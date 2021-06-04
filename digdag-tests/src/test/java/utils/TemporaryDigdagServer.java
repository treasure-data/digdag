package utils;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.Version;
import io.digdag.core.database.DataSourceProvider;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.core.database.RemoteDatabaseConfig;
import io.digdag.server.ServerRuntimeInfo;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.ws.rs.ProcessingException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.digdag.client.DigdagVersion.buildVersion;
import static io.digdag.client.DigdagVersion.VERSION_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static utils.TestUtils.configFactory;
import static utils.TestUtils.main;
import static utils.TestUtils.objectMapper;

public class TemporaryDigdagServer
        implements TestRule, AutoCloseable
{
    private static final boolean IN_PROCESS_DEFAULT = Boolean.valueOf(System.getenv().getOrDefault("DIGDAG_TEST_TEMP_SERVER_IN_PROCESS", "true"));

    private static final String POSTGRESQL = System.getenv("DIGDAG_TEST_POSTGRESQL");

    private static final String JACOCO_JVM_ARG = System.getenv("JACOCO_JVM_ARG");  // set at build.gradle

    private static final Logger log = LoggerFactory.getLogger(TemporaryDigdagServer.class);

    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true).build();

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final Optional<Version> version;

    private final String commandMainClassName;
    private final String host;
    private final List<String> extraArgs;

    private final ExecutorService executor;
    private final List<String> configuration;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private final boolean inProcess;
    private final Map<String, String> environment;
    private final Properties properties;

    private Path workdir;
    private Process serverProcess;

    private Path configDirectory;
    private Path config;
    private Path taskLog;
    private Path accessLog;

    private DatabaseConfig adminDatabaseConfig;
    private DatabaseConfig testDatabaseConfig;
    private DataSource adminDataSource;

    private int port = -1;
    private int adminPort = -1;

    private boolean started;
    private boolean closed;

    public TemporaryDigdagServer(Builder builder)
    {
        this.version = builder.version;

        this.commandMainClassName = builder.commandMainClassName;
        this.host = "127.0.0.1";
        this.configuration = new ArrayList<>(Objects.requireNonNull(builder.configuration, "configuration"));
        this.extraArgs = ImmutableList.copyOf(Objects.requireNonNull(builder.args, "args"));
        this.inProcess = builder.inProcess;
        this.environment = ImmutableMap.copyOf(builder.environment);
        this.properties = builder.properties;

        this.executor = Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);

        if (!isNullOrEmpty(POSTGRESQL)) {
            preparePostgres();
        }
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {
            @Override
            public void evaluate()
                    throws Throwable
            {
                before();
                try {
                    base.evaluate();
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

    /**
     * Get DataSource for Test DB. (Not admin DB)
     * @return
     */
    public DataSource getTestDBDataSource()
    {
        if (isRemoteDatabase()) {
            return new DataSourceProvider(testDatabaseConfig).get();
        }
        else {
            return null;
        }
    }

    public DatabaseConfig getTestDatabaseConfig() { return testDatabaseConfig; }



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

        public static void main(final String... args)
        {
            err("child process started");
            Watchdog watchdog = new Watchdog();
            watchdog.start();

            try {
                final String commandMainClassName = args[0];
                final String[] methodArgs = new String[args.length - 1];
                System.arraycopy(args, 1, methodArgs, 0, methodArgs.length);

                TemporaryDigdagServer.class.getClassLoader()
                        .loadClass(commandMainClassName)
                        .getDeclaredMethod("main", String[].class)
                        .invoke(null, (Object)methodArgs);
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
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
        if (started) {
            return;
        }
        start();
    }
    public void start()
            throws Exception
    {
        start(false);
    }

    public void start(boolean skipSetupDatabase)
            throws Exception
    {
        started = true;

        temporaryFolder.create();

        if (!skipSetupDatabase) {
            setupDatabase();
        }

        Path runtimeInfoPath = temporaryFolder.newFolder().toPath().resolve("runtime-info");
        configuration.add("server.runtime-info.path = " + runtimeInfoPath.toAbsolutePath().normalize());
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
                "-l", "info",
                "--port", "0",
                "--bind", host,
                "--admin-port", "0",
                "--admin-bind", host,
                "--access-log", accessLog.toString(),
                "-c", config.toString()));
        if (!configuration.stream().anyMatch(c -> c.contains("log-server.type"))) {
            argsBuilder.addAll(ImmutableList.of(
                    "--task-log", taskLog.toString()));
        }
        argsBuilder.addAll(extraArgs);

        List<String> args = argsBuilder.build();

        if (inProcess) {
            InputStream in = new ByteArrayInputStream(new byte[0]);
            executor.execute(() -> {
                OutputStream out = fanout(this.out, System.out);
                OutputStream err = fanout(this.err, System.err);
//                main(environment, LocalVersion.of(version.or(buildVersion())), args, out, err, in);
            });
        }
        else {
            File workdir = temporaryFolder.newFolder();
            String home = System.getProperty("java.home");
            String classPath = System.getProperty("java.class.path");
            Path java = Paths.get(home, "bin", "java").toAbsolutePath().normalize();

            List<String> processArgs = new ArrayList<>();
            processArgs.add(java.toString());
            processArgs.addAll(asList(
                    "-cp", classPath,
                    "-XX:TieredStopAtLevel=1", "-Xverify:none",  // for faster startup and less memory consumption on CI
                    "-Djdk.attach.allowAttachSelf=true", // for io.digdag.guice.rs.server.jmx.JmxAgent
                    "-Xms128m", "-Xmx128m"));
            if (version.isPresent()) {
                processArgs.add("-D" + VERSION_PROPERTY + "=" + version.get());
            }
            for (String key : properties.stringPropertyNames()) {
                processArgs.add("-D" + key + "=" + properties.getProperty(key));
            }
            if (!isNullOrEmpty(JACOCO_JVM_ARG)) {
                log.debug("TEMP_JACOCO_JVM_ARG: {}", JACOCO_JVM_ARG);
                processArgs.add(JACOCO_JVM_ARG);
            }
            processArgs.add(Trampoline.class.getName());
            processArgs.add(commandMainClassName);
            processArgs.addAll(args);

            ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

            processBuilder.environment().clear();
            processBuilder.environment().putAll(environment);

            processBuilder.directory(workdir);
            serverProcess = processBuilder.start();

            executor.execute(() -> copy(serverProcess.getInputStream(), out, System.out));
            executor.execute(() -> copy(serverProcess.getErrorStream(), err, System.err));
        }
        if (true) return;

        // Wait for server to write the server info with the local address
        ServerRuntimeInfo serverRuntimeInfo = null;
        for (int i = 0; i < 300; i++) {
            Thread.sleep(1000);
            if (serverProcess != null && !serverProcess.isAlive()) {
                break;
            }
            if (!Files.exists(runtimeInfoPath)) {
                continue;
            }
            try {
                serverRuntimeInfo = objectMapper().readValue(runtimeInfoPath.toFile(), ServerRuntimeInfo.class);
                break;
            }
            catch (IOException e) {
                continue;
            }
        }

        if (serverRuntimeInfo == null) {
            fail("Server failed to come up.\nout:\n" + out.toString("UTF-8") + "\nerr:\n" + err.toString("UTF-8"));
        }

        assert !serverRuntimeInfo.localAddresses().isEmpty();
        assert !serverRuntimeInfo.localAdminAddresses().isEmpty();
        port = serverRuntimeInfo.localAddresses().get(0).port();
        adminPort = serverRuntimeInfo.localAdminAddresses().get(0).port();

        // Poll and wait for server to respond
        boolean up = false;
        for (int i = 0; i < 300; i++) {
            try (DigdagClient client = DigdagClient.builder()
                    .host(host)
                    .port(port)
                    .build()) {
                client.getVersion();
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

    private static OutputStream fanout(OutputStream... outputStreams)
    {
        return new OutputStream()
        {
            @Override
            public void write(int b)
                    throws IOException
            {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.write(b);
                }
            }

            @Override
            public void write(byte[] b)
                    throws IOException
            {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.write(b);
                }
            }

            @Override
            public void write(byte[] b, int off, int len)
                    throws IOException
            {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.write(b, off, len);
                }
            }

            @Override
            public void flush()
                    throws IOException
            {
                for (OutputStream outputStream : outputStreams) {
                    outputStream.flush();
                }
            }
        };
    }

    private void copy(InputStream in, OutputStream... outs)
    {
        byte[] buffer = new byte[16 * 1024];
        try {
            while (true) {
                int r = in.read(buffer);
                if (r < 0) {
                    break;
                }
                for (OutputStream out : outs) {
                    out.write(buffer, 0, r);
                    out.flush();
                }
            }
        }
        catch (IOException e) {
            log.error("Caught exception during byte stream copy", e);
        }
    }

    private static void kill(Process p)
    {
        // Send TERM first and wait for a while so that shutdown handler runs
        // and Jacoco agent can write coverage data.
        boolean exited = false;
        if (terminate(p)) {
            try {
                exited = p.waitFor(5, SECONDS);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                // kill anyways
            }
        }
        if (!exited) {
            sendUnixSignal(p, "KILL");
            p.destroyForcibly();
        }
    }

    private static boolean terminate(Process p)
    {
        return sendUnixSignal(p, "TERM");
    }

    private static boolean sendUnixSignal(Process p, String signalName)
    {
        if (isUnixProcess(p)) {
            int pid = pid(p);
            if (pid != -1) {
                String[] cmd = {"kill", "-s", signalName, Integer.toString(pid)};
                try {
                    Runtime.getRuntime().exec(cmd);
                }
                catch (IOException e) {
                    log.warn("command failed: {}", asList(cmd), e);
                }
            }
            return true;
        }
        else {
            return false;
        }
    }

    private static int pid(Process p)
    {
        if (isUnixProcess(p)) {
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

    private static boolean isUnixProcess(Process p)
    {
        return p.getClass().getName().equals("java.lang.UNIXProcess");
    }

    public void setupDatabase()
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
        if (!started || closed) {
            return;
        }
        close();
    }

    @Override
    public void close()
    {
        closed = true;

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

        temporaryFolder.delete();
    }

    public boolean isRemoteDatabase()
    {
        return testDatabaseConfig != null && testDatabaseConfig.getRemoteDatabaseConfig().isPresent();
    }

    public DatabaseConfig getRemoteTestDatabaseConfig()
    {
        return testDatabaseConfig;
    }

    public boolean hasUnixProcess()
    {
        return serverProcess != null && isUnixProcess(serverProcess);
    }

    public void terminateProcess()
    {
        if (!hasUnixProcess()) {
            throw new IllegalStateException("server doesn't have UNIX process");
        }
        terminate(serverProcess);
    }

    public boolean isProcessAlive()
    {
        if (!hasUnixProcess()) {
            throw new IllegalStateException("server doesn't have UNIX process");
        }
        return serverProcess.isAlive();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public String endpoint()
    {
        return "http://" + host + ":" + port();
    }

    public String host()
    {
        return host;
    }

    public int port()
    {
        if (port == -1) {
            throw new IllegalStateException("server not yet up");
        }
        return port;
    }

    public int adminPort()
    {
        if (adminPort == -1) {
            throw new IllegalStateException("server not yet up");
        }
        return adminPort;
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

    public static class Builder
    {
        private Builder()
        {
        }

        private String commandMainClassName = io.digdag.cli.Main.class.getName();
        private List<String> args = new ArrayList<>();
        private Optional<Version> version = Optional.absent();
        private List<String> configuration = new ArrayList<>();
        private Map<String, String> environment = new HashMap<>();
        private Properties properties = new Properties();
        private boolean inProcess = IN_PROCESS_DEFAULT;
        private byte[] stdin = new byte[0];

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

        public Builder environment(Map<String, String> environment)
        {
            this.environment.putAll(environment);
            return this;
        }

        public Builder commandMainClassName(final String className)
        {
            this.commandMainClassName = className;
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

        public Builder systemProperty(String key, String value)
        {
            properties.setProperty(key, value);
            return this;
        }

        public Builder withRandomSecretEncryptionKey()
        {
            byte[] key = new byte[128 / 8];
            ThreadLocalRandom.current().nextBytes(key);
            String keyBase64 = Base64.getEncoder().encodeToString(key);
            return configuration("digdag.secret-encryption-key = " + keyBase64);
        }

        public TemporaryDigdagServer build()
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
