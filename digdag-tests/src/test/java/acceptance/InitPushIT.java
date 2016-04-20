package acceptance;

import io.digdag.client.DigdagClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.digdag.cli.Main.main;

public class InitPushIT {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;
    private Path clientProperties;
    private Path serverProperties;
    private Path project;

    private String host;
    private int port;
    private String endpoint;

    @Before
    public void setUp() throws Exception {
        project = folder.getRoot().toPath().resolve("foobar");
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
        main("init", project.toString());
        main("push",
                "foobar",
                "-f", project.resolve("digdag.yml").toString(),
                "-e", endpoint,
                "-r", "1");
    }
}
