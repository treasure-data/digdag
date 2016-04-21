package acceptance;

import io.digdag.cli.Main;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static acceptance.TestUtils.copyResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConfigIT
{
    public static int main(String... args)
    {
        return new Main().cli(args);
    }

    private interface Action
    {
        public void run() throws Exception;
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private void fakeHome(String home, Action a) throws Exception
    {
        String orig = System.setProperty("user.home", home);
        try {
            Files.createDirectories(Paths.get(home).resolve(".digdag"));
            a.run();
        }
        finally {
            System.setProperty("user.home", orig);
        }
    }

    @Test
    public void propertyByFile() throws Exception
    {
        copyResource("acceptance/params.yml", root().resolve("params.yml"));
        fakeHome(root().resolve("home").toString(), () -> {
            Files.write(root().resolve("home").resolve(".digdag").resolve("config"), "params.mysql.password=secret".getBytes(UTF_8));
            main("run", "-o", root().toString(), "-f", root().resolve("params.yml").toString());
        });
        assertThat(Files.readAllBytes(root().resolve("foo.out")), is("secret\n".getBytes(UTF_8)));
    }
}
