package acceptance;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.client.DigdagClient;
import io.digdag.core.Version;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;

import static acceptance.TestUtils.findFreePort;
import static acceptance.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TemporaryDigdagServer
        implements TestRule
{
    private static final Logger log = LoggerFactory.getLogger(TemporaryDigdagServer.class);

    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true).build();

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final Version version;

    private final String host;
    private final int port;
    private final String endpoint;
    private final List<String> extraArgs;

    private final ExecutorService executor;
    private final String configuration;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private Path configDirectory;
    private Path config;
    private Path taskLog;
    private Path accessLog;

    public TemporaryDigdagServer(Builder builder)
    {
        this.version = Objects.requireNonNull(builder.version, "version");

        this.host = "127.0.0.1";
        // TODO (dano): Ideally the server could use system port allocation (bind on port 0) and tell
        //              us what port it got. That way we'd not have any spurious port collisions.
        this.port = findFreePort();
        this.endpoint = "http://" + host + ":" + port;
        this.configuration = Objects.requireNonNull(builder.configuration, "configuration");
        this.extraArgs = ImmutableList.copyOf(Objects.requireNonNull(builder.args, "args"));

        this.executor = Executors.newSingleThreadExecutor(DAEMON_THREAD_FACTORY);
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

    private void before()
            throws Throwable
    {
        try {
            this.configDirectory = temporaryFolder.newFolder().toPath();
            this.taskLog = temporaryFolder.newFolder().toPath();
            this.accessLog = temporaryFolder.newFolder().toPath();
            this.config = Files.write(configDirectory.resolve("config"), configuration.getBytes(UTF_8));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        List<String> args = Stream.concat(Stream.of(
                "server",
                "-m",
                "--port", String.valueOf(port),
                "--bind", host,
                "--task-log", taskLog.toString(),
                "--access-log", accessLog.toString(),
                "-c", config.toString()),
                extraArgs.stream())
                .collect(toList());

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

    private void after()
    {
        executor.shutdownNow();
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
        return out(StandardCharsets.UTF_8);
    }

    public String errUtf8()
    {
        return err(StandardCharsets.UTF_8);
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
        private String configuration = "";

        public Builder version(Version version)
        {
            this.version = version;
            return this;
        }

        public Builder configuration(String... configuration)
        {
            this.configuration = Joiner.on('\n').join(configuration);
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
