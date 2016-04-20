package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.digdag.cli.Main.main;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class InitPushIT {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;
    private Path clientProperties;
    private Path serverProperties;
    private Path projectDir;

    private String host;
    private int port;
    private String endpoint;

    @Before
    public void setUp() throws Exception {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        clientProperties = Files.createFile(folder.getRoot().toPath().resolve("client.properties"));
        serverProperties = Files.createFile(folder.getRoot().toPath().resolve("server.properties"));
        executor = Executors.newCachedThreadPool();
        executor.execute(() -> main("server", "-m", "-c", serverProperties.toString()));

        host = "localhost";
        port = 65432;
        endpoint = "http://" + host + ":" + port;

        // Poll and wait for server to come up
        for (int i = 0; i < 30; i++) {
            DigdagClient client = DigdagClient.builder()
                    .host(host)
                    .port(port)
                    .build();
            try {
                client.getProjects();
                break;
            } catch (Exception e) {
                System.out.println(".");
            }
            Thread.sleep(1000);
        }
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void initAndPush() throws Exception {
        main("init", projectDir.toString());
        main("push",
                "foobar",
                "-f", projectDir.resolve("digdag.yml").toString(),
                "-e", endpoint,
                "-r", "4711");
        DigdagClient client = DigdagClient.builder()
                .host(host)
                .port(port)
                .build();

        RestProject project = client.getProject("foobar");

        assertThat(project.getName(), is("foobar"));
        assertThat(project.getRevision(), is("4711"));
        long now = Instant.now().toEpochMilli();
        long error = MINUTES.toMillis(1);
        assertThat(project.getCreatedAt().toEpochMilli(), is(both(
                greaterThan(now - error))
                .and(lessThan(now + error))));
    }
}
